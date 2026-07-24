//! Resolver 2a — **Codex coder (maker) routing** (`agents/processor.md`, "Codex-исполнитель
//! (coder_codex) и маршрутизация"; phases 2.2 / 2.8).
//!
//! `coder_codex` is an optional accelerator: it takes low/medium-complexity implementation and
//! `R-`-fix work off Claude, gated on `CODEX_CODER`. Routing a task to it is a THREE-stage
//! decision, evaluated in order and short-circuiting on the first stage that keeps the work on
//! Claude:
//!
//! 1. **Level resolver** — `CODEX_CODER` × `Рекомендуемый исполнитель`. `coder_deep` is always
//!    Claude; `off` is always Claude.
//! 2. **Network gate** (only when the descriptor carries `Сеть: требуется` + `Экосистема:`) —
//!    if the task needs the network and no Codex network path exists for its ecosystem right
//!    now, keep it on Claude rather than burn an empty Codex run.
//! 3. **KB `ENV_LIMIT` pitfall** (only when KB is on and a pitfall's scope intersects the task)
//!    — a known environment escalation for this scope routes back to Claude before the call.
//!
//! Every stage is a pure decision made BEFORE any Codex process is spawned; a decline is not a
//! Codex attempt. All I/O (reading the descriptor, scanning `.work/knowledge/`) happens in the
//! caller — this resolver only transforms the already-parsed inputs.

use super::vocab::Level;

/// The `CODEX_CODER` routing flag (`config.example.md`: `off` / `fast` / `fast+std`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexCoder {
    Off,
    Fast,
    FastStd,
}

impl CodexCoder {
    /// Parse a config value; unrecognized/empty → `None` (caller applies the `off` default).
    pub fn parse(value: &str) -> Option<CodexCoder> {
        Some(match value.trim() {
            "off" => CodexCoder::Off,
            "fast" => CodexCoder::Fast,
            "fast+std" => CodexCoder::FastStd,
            _ => return None,
        })
    }
}

/// The task's ecosystem class for the network gate (`agents/processor.md`, "Сетевой гейт").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ecosystem {
    /// `cargo` / `npm` / `pip` / `uv` / `nuget` — served by the T-063 network broker **unconditionally**
    /// (a Codex network path always exists, independent of `CODEX_NETWORK`).
    Managed,
    /// `прочее` — arbitrary network beyond the broker allowlist; reachable only over Codex's
    /// direct sandbox network, i.e. only when `CODEX_NETWORK: on`.
    Other,
}

impl Ecosystem {
    /// Map an `Экосистема:` literal: the broker-served managers → `Managed`, anything else
    /// (including the literal `прочее`) → `Other`.
    pub fn parse(value: &str) -> Ecosystem {
        match value.trim() {
            "cargo" | "npm" | "pip" | "uv" | "nuget" => Ecosystem::Managed,
            _ => Ecosystem::Other,
        }
    }

    /// Is a Codex network path available for this ecosystem right now? Broker-served managers:
    /// always. `Other`: only on the direct sandbox network (`CODEX_NETWORK: on`).
    fn path_available(self, codex_network: bool) -> bool {
        match self {
            Ecosystem::Managed => true,
            Ecosystem::Other => codex_network,
        }
    }
}

/// A task's declared network requirement: `Сеть: требуется` + its `Экосистема:`. The gate only
/// applies when this is present; its absence is the pre-T-064 behavior (route by `CODEX_CODER`
/// alone, no network gate).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NetworkNeed {
    pub ecosystem: Ecosystem,
}

/// Build a [`NetworkNeed`] from the descriptor's `Сеть:` and `Экосистема:` fields. Returns
/// `Some` only when `Сеть:` explicitly reads `требуется`; any other/absent value → `None` (no
/// gate). A missing `Экосистема:` alongside `Сеть: требуется` conservatively reads as `Other`
/// (no evidence of a broker path).
pub fn network_need(set_literal: Option<&str>, eco_literal: Option<&str>) -> Option<NetworkNeed> {
    match set_literal.map(str::trim) {
        Some("требуется") => Some(NetworkNeed {
            ecosystem: eco_literal
                .map(Ecosystem::parse)
                .unwrap_or(Ecosystem::Other),
        }),
        _ => None,
    }
}

/// A known Codex `ENV_LIMIT/<class>` environment escalation, recorded as a KB `pitfall` whose
/// scope intersects the task (`agents/processor.md`, "Сверка с ENV_LIMIT-pitfall KB").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvLimitClass {
    /// `vcs-write` — never resolvable by the current harness.
    VcsWrite,
    /// `network` — resolvable only if a Codex network path is available now.
    Network,
    /// `tls-schannel` — resolvable only if a Codex network path is available now.
    TlsSchannel,
    /// `profile-denied` — treated conservatively as unresolvable.
    ProfileDenied,
    /// `sandbox-init-worktree` — a retained nested-worktree failure requires one serialized live
    /// canary before the session may re-enable Codex coders.
    SandboxInitWorktree,
    /// An unknown / newly-seen class — treated conservatively as unresolvable.
    Unknown,
}

impl EnvLimitClass {
    /// Map the `<class>` token of an `ENV_LIMIT/<class>` literal; anything unrecognized →
    /// `Unknown` (handled as conservatively as `vcs-write`).
    pub fn parse(class: &str) -> EnvLimitClass {
        match class.trim() {
            "vcs-write" => EnvLimitClass::VcsWrite,
            "network" => EnvLimitClass::Network,
            "tls-schannel" => EnvLimitClass::TlsSchannel,
            "profile-denied" => EnvLimitClass::ProfileDenied,
            "sandbox-init-worktree" => EnvLimitClass::SandboxInitWorktree,
            _ => EnvLimitClass::Unknown,
        }
    }
}

/// The typed inputs of the coder-routing decision — the parsed descriptor fields and config
/// keys, never raw text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoderRouteInput {
    /// `CODEX_CODER`.
    pub codex_coder: CodexCoder,
    /// `Рекомендуемый исполнитель`.
    pub level: Level,
    /// `CODEX_NETWORK` (governs the direct sandbox network path).
    pub codex_network: bool,
    /// The task's `Сеть:`/`Экосистема:` requirement, or `None` when the field is absent.
    pub network: Option<NetworkNeed>,
    /// A KB `ENV_LIMIT` pitfall class intersecting the task's scope, or `None` (KB off, no
    /// `.work/knowledge/`, or no matching record — the unchanged-behavior case).
    pub kb_pitfall: Option<EnvLimitClass>,
}

/// Why a task stayed on Claude instead of being routed to `coder_codex`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StayClaude {
    /// `CODEX_CODER=off`.
    CodexOff,
    /// The level is outside the flag's set (`coder`/`coder_deep` under `fast`, or any
    /// `coder_deep` — which is always Claude).
    LevelExcluded,
    /// The network gate: the task needs the network and no Codex path exists for its ecosystem.
    NetworkGate,
    /// A KB `ENV_LIMIT` pitfall for this scope is unresolvable by the current harness.
    KbPitfall(EnvLimitClass),
}

/// The coder-routing outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoderRoute {
    /// Route to `coder_codex`.
    Codex,
    /// Keep the work on the Claude coder of the task's level, with the reason Codex was skipped.
    Claude(StayClaude),
}

/// Resolve where a task's implementation / `R-`-fix goes: `coder_codex` or the Claude coder of
/// its level. Pure over [`CoderRouteInput`]; evaluates the level resolver, then the network
/// gate, then the KB pitfall check, short-circuiting on the first stage that keeps it on Claude.
pub fn route_coder(inp: &CoderRouteInput) -> CoderRoute {
    // Stage 1 — level resolver (CODEX_CODER × level). `coder_deep` is always Claude; so is `off`.
    match inp.codex_coder {
        CodexCoder::Off => return CoderRoute::Claude(StayClaude::CodexOff),
        CodexCoder::Fast => {
            if inp.level != Level::CoderFast {
                return CoderRoute::Claude(StayClaude::LevelExcluded);
            }
        }
        CodexCoder::FastStd => {
            if !matches!(inp.level, Level::CoderFast | Level::Coder) {
                return CoderRoute::Claude(StayClaude::LevelExcluded);
            }
        }
    }

    // Stage 2 — network gate (only when the descriptor declares a `Сеть:` requirement).
    if let Some(need) = inp.network
        && !need.ecosystem.path_available(inp.codex_network)
    {
        return CoderRoute::Claude(StayClaude::NetworkGate);
    }

    // Stage 3 — KB `ENV_LIMIT` pitfall for this scope.
    if let Some(class) = inp.kb_pitfall
        && pitfall_blocks(class, inp.network, inp.codex_network)
    {
        return CoderRoute::Claude(StayClaude::KbPitfall(class));
    }

    CoderRoute::Codex
}

/// Does a KB `ENV_LIMIT` pitfall class block Codex for this task? `vcs-write` / `profile-denied`
/// / unknown are never resolvable; `network` / `tls-schannel` block only when no Codex network
/// path is available now (same availability rule as the network gate — a task with no `Сеть:`
/// field is treated as `Other`, i.e. reachable only over the direct sandbox network).
fn pitfall_blocks(class: EnvLimitClass, network: Option<NetworkNeed>, codex_network: bool) -> bool {
    match class {
        EnvLimitClass::VcsWrite | EnvLimitClass::ProfileDenied | EnvLimitClass::Unknown => true,
        // The stateful headless adapter owns the one-task session canary. Treating this class as
        // a permanent pure-route prohibition would make successful revalidation impossible.
        EnvLimitClass::SandboxInitWorktree => false,
        EnvLimitClass::Network | EnvLimitClass::TlsSchannel => {
            let eco = network.map(|n| n.ecosystem).unwrap_or(Ecosystem::Other);
            !eco.path_available(codex_network)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base(codex_coder: CodexCoder, level: Level) -> CoderRouteInput {
        CoderRouteInput {
            codex_coder,
            level,
            codex_network: true,
            network: None,
            kb_pitfall: None,
        }
    }

    #[test]
    fn config_and_ecosystem_and_class_parse() {
        assert_eq!(CodexCoder::parse("fast+std"), Some(CodexCoder::FastStd));
        assert_eq!(CodexCoder::parse(" off "), Some(CodexCoder::Off));
        assert_eq!(CodexCoder::parse("deep"), None); // not a coder value
        assert_eq!(Ecosystem::parse("cargo"), Ecosystem::Managed);
        assert_eq!(Ecosystem::parse("uv"), Ecosystem::Managed);
        assert_eq!(Ecosystem::parse("nuget"), Ecosystem::Managed);
        assert_eq!(Ecosystem::parse("прочее"), Ecosystem::Other);
        assert_eq!(EnvLimitClass::parse("vcs-write"), EnvLimitClass::VcsWrite);
        assert_eq!(
            EnvLimitClass::parse("tls-schannel"),
            EnvLimitClass::TlsSchannel
        );
        assert_eq!(
            EnvLimitClass::parse("sandbox-init-worktree"),
            EnvLimitClass::SandboxInitWorktree
        );
        assert_eq!(EnvLimitClass::parse("brand-new"), EnvLimitClass::Unknown);
    }

    #[test]
    fn network_need_only_when_required() {
        // No `Сеть:` field → no gate.
        assert_eq!(network_need(None, Some("cargo")), None);
        // `Сеть: требуется` + ecosystem → typed need.
        assert_eq!(
            network_need(Some("требуется"), Some("cargo")),
            Some(NetworkNeed {
                ecosystem: Ecosystem::Managed
            })
        );
        // Required but no ecosystem → conservative Other.
        assert_eq!(
            network_need(Some("требуется"), None),
            Some(NetworkNeed {
                ecosystem: Ecosystem::Other
            })
        );
        // A non-`требуется` value is not a requirement.
        assert_eq!(network_need(Some("не требуется"), Some("npm")), None);
    }

    /// Stage 1 — every (CODEX_CODER × level) cell of the level resolver.
    #[test]
    fn level_resolver_table() {
        use CodexCoder::*;
        use Level::*;
        let cases = [
            // off → always Claude.
            (Off, CoderFast, CoderRoute::Claude(StayClaude::CodexOff)),
            (Off, Coder, CoderRoute::Claude(StayClaude::CodexOff)),
            (Off, CoderDeep, CoderRoute::Claude(StayClaude::CodexOff)),
            // fast → only coder_fast to Codex.
            (Fast, CoderFast, CoderRoute::Codex),
            (Fast, Coder, CoderRoute::Claude(StayClaude::LevelExcluded)),
            (
                Fast,
                CoderDeep,
                CoderRoute::Claude(StayClaude::LevelExcluded),
            ),
            // fast+std → coder_fast and coder to Codex; coder_deep never.
            (FastStd, CoderFast, CoderRoute::Codex),
            (FastStd, Coder, CoderRoute::Codex),
            (
                FastStd,
                CoderDeep,
                CoderRoute::Claude(StayClaude::LevelExcluded),
            ),
        ];
        for (cc, level, want) in cases {
            assert_eq!(
                route_coder(&base(cc, level)),
                want,
                "{cc:?} × {}",
                level.as_str()
            );
        }
    }

    /// Stage 2 — network gate over the ecosystem × `CODEX_NETWORK` grid (level resolver already
    /// says Codex).
    #[test]
    fn network_gate_table() {
        // Managed ecosystem: broker path always exists → Codex regardless of CODEX_NETWORK.
        for net in [true, false] {
            let inp = CoderRouteInput {
                network: Some(NetworkNeed {
                    ecosystem: Ecosystem::Managed,
                }),
                codex_network: net,
                ..base(CodexCoder::Fast, Level::CoderFast)
            };
            assert_eq!(route_coder(&inp), CoderRoute::Codex, "managed net={net}");
        }
        // Other ecosystem: only reachable when CODEX_NETWORK on.
        let other_on = CoderRouteInput {
            network: Some(NetworkNeed {
                ecosystem: Ecosystem::Other,
            }),
            codex_network: true,
            ..base(CodexCoder::Fast, Level::CoderFast)
        };
        assert_eq!(route_coder(&other_on), CoderRoute::Codex);
        let other_off = CoderRouteInput {
            codex_network: false,
            ..other_on
        };
        assert_eq!(
            route_coder(&other_off),
            CoderRoute::Claude(StayClaude::NetworkGate)
        );
        // No `Сеть:` field → gate does not apply even with CODEX_NETWORK off.
        let no_field = CoderRouteInput {
            network: None,
            codex_network: false,
            ..base(CodexCoder::Fast, Level::CoderFast)
        };
        assert_eq!(route_coder(&no_field), CoderRoute::Codex);
    }

    /// Stage 3 — KB `ENV_LIMIT` pitfall over every class, with and without a live network path.
    #[test]
    fn kb_pitfall_table() {
        // Always-unresolvable classes block regardless of network.
        for class in [
            EnvLimitClass::VcsWrite,
            EnvLimitClass::ProfileDenied,
            EnvLimitClass::Unknown,
        ] {
            let inp = CoderRouteInput {
                kb_pitfall: Some(class),
                ..base(CodexCoder::FastStd, Level::Coder)
            };
            assert_eq!(
                route_coder(&inp),
                CoderRoute::Claude(StayClaude::KbPitfall(class)),
                "{class:?}"
            );
        }

        let canary = CoderRouteInput {
            kb_pitfall: Some(EnvLimitClass::SandboxInitWorktree),
            ..base(CodexCoder::FastStd, Level::Coder)
        };
        assert_eq!(route_coder(&canary), CoderRoute::Codex);
        // network/tls classes: blocked when no path (Other + CODEX_NETWORK off), cleared when a
        // path exists (managed broker, or Other + CODEX_NETWORK on).
        for class in [EnvLimitClass::Network, EnvLimitClass::TlsSchannel] {
            // Managed ecosystem → broker path → not blocked.
            let managed = CoderRouteInput {
                kb_pitfall: Some(class),
                network: Some(NetworkNeed {
                    ecosystem: Ecosystem::Managed,
                }),
                codex_network: false,
                ..base(CodexCoder::FastStd, Level::Coder)
            };
            assert_eq!(
                route_coder(&managed),
                CoderRoute::Codex,
                "{class:?} managed"
            );
            // No `Сеть:` field + CODEX_NETWORK on → direct path → not blocked.
            let net_on = CoderRouteInput {
                kb_pitfall: Some(class),
                network: None,
                codex_network: true,
                ..base(CodexCoder::FastStd, Level::Coder)
            };
            assert_eq!(route_coder(&net_on), CoderRoute::Codex, "{class:?} net-on");
            // No path (Other, CODEX_NETWORK off) → blocked. (Here the network gate itself would
            // not fire because there is no `Сеть:` field, so the KB check is what blocks.)
            let no_path = CoderRouteInput {
                kb_pitfall: Some(class),
                network: None,
                codex_network: false,
                ..base(CodexCoder::FastStd, Level::Coder)
            };
            assert_eq!(
                route_coder(&no_path),
                CoderRoute::Claude(StayClaude::KbPitfall(class)),
                "{class:?} no-path"
            );
        }
    }

    #[test]
    fn stage_precedence_off_and_level_beat_gates() {
        // `off` short-circuits before any gate is consulted.
        let off = CoderRouteInput {
            network: Some(NetworkNeed {
                ecosystem: Ecosystem::Other,
            }),
            codex_network: false,
            kb_pitfall: Some(EnvLimitClass::VcsWrite),
            ..base(CodexCoder::Off, Level::CoderFast)
        };
        assert_eq!(route_coder(&off), CoderRoute::Claude(StayClaude::CodexOff));
        // coder_deep is excluded at stage 1 even under fast+std, before gates.
        let deep = CoderRouteInput {
            kb_pitfall: Some(EnvLimitClass::VcsWrite),
            ..base(CodexCoder::FastStd, Level::CoderDeep)
        };
        assert_eq!(
            route_coder(&deep),
            CoderRoute::Claude(StayClaude::LevelExcluded)
        );
        // Network gate wins over a later KB pitfall (evaluated first).
        let gate_first = CoderRouteInput {
            network: Some(NetworkNeed {
                ecosystem: Ecosystem::Other,
            }),
            codex_network: false,
            kb_pitfall: Some(EnvLimitClass::VcsWrite),
            ..base(CodexCoder::Fast, Level::CoderFast)
        };
        assert_eq!(
            route_coder(&gate_first),
            CoderRoute::Claude(StayClaude::NetworkGate)
        );
    }
}
