-------------------------- MODULE KafkaMetaResolution --------------------------
(***************************************************************************)
(* Kafka meta column resolution for one Kafka table q, one MergeTree table *)
(* t and an optional explicit materialized view v (q -> t).                *)
(*                                                                         *)
(* Code: KafkaMetaColumnFactory.Resolve, ClickHouseSchemaGenerator         *)
(* (ResolveMetaForMergeTree / ResolveMetaForMaterializedView),             *)
(* PipelineColumnExpander, MaterializedViewAutoGenerator.                  *)
(*                                                                         *)
(* Impl = "buggy" is the code before PR #57 (defects 1.1 / 1.2),           *)
(* Impl = "fixed" is the current code.                                     *)
(*                                                                         *)
(* Every initial state is one configuration; the plan is a pure function   *)
(* of it, so TLC checks the invariants on every configuration.             *)
(***************************************************************************)
EXTENDS FiniteSets, Sequences

CONSTANTS Impl, Flags

ASSUME Impl \in {"buggy", "fixed"}

Tri == {"true", "false", "null"}

VARIABLES defaults, queueOverride, tInclude, tAutoColumns, tHasSource,
          hasView, vInclude, vAutoColumns

vars == <<defaults, queueOverride, tInclude, tAutoColumns, tHasSource,
          hasView, vInclude, vAutoColumns>>

Payload == "payload"
MetaCol(f) == "kafka_" \o f
Virtual(f) == "_" \o f
AllMetaCols == {MetaCol(f) : f \in Flags}
KafkaSelectable == {Payload} \cup {Virtual(f) : f \in Flags}

Coalesce(a, b) == IF a # "null" THEN a ELSE b

Res(base, include) ==
    IF include = "false" THEN {}
    ELSE IF include = "true" /\ base = {} THEN {"key", "headers"} \cap Flags
    ELSE base

QueueMeta ==
    {f \in Flags : queueOverride[f] = "true"
                   \/ (queueOverride[f] = "null" /\ f \in defaults)}

TSourceKnown == tHasSource \/ hasView

TMeta ==
    IF Impl = "buggy"
    THEN Res(IF tHasSource THEN QueueMeta ELSE defaults, tInclude)
    ELSE Res(IF TSourceKnown THEN QueueMeta ELSE defaults, tInclude)

TEffective == IF Impl = "buggy" THEN TMeta ELSE Res(TMeta, tInclude)

MetaCols(m) == {MetaCol(f) : f \in m}
MetaMappings(m) == {<<Virtual(f), MetaCol(f)>> : f \in m}

TCols ==
    {Payload} \cup
      (IF tAutoColumns \/ tInclude = "true" THEN MetaCols(TEffective) ELSE {})

AutoViewExists == tAutoColumns /\ tHasSource /\ ~hasView

AutoViewMeta ==
    IF Impl = "buggy" THEN QueueMeta ELSE Res(QueueMeta, tInclude)

AutoViewMappings ==
    LET copied == {<<c, c>> : c \in TCols}
    IN IF AutoViewMeta # {}
       THEN {m \in copied : m[2] \notin AllMetaCols} \cup MetaMappings(AutoViewMeta)
       ELSE copied

ViewIncludeOrTarget == Coalesce(vInclude, tInclude)

ViewMeta ==
    IF Impl = "buggy"
    THEN TMeta
    ELSE Res(QueueMeta, ViewIncludeOrTarget)

ViewEffective == IF Impl = "buggy" THEN ViewMeta ELSE Res(ViewMeta, ViewIncludeOrTarget)

ViewMappings ==
    {<<Payload, Payload>>} \cup
      (IF vAutoColumns
       THEN MetaMappings(ViewEffective)
       ELSE IF vInclude = "true" \/ (vInclude = "null" /\ tInclude = "true")
            THEN MetaMappings(ViewEffective)
            ELSE {})

ViewMetaTargets == {m[2] : m \in ViewMappings} \cap AllMetaCols

Init ==
    /\ defaults \in SUBSET Flags
    /\ queueOverride \in [Flags -> Tri]
    /\ tInclude \in Tri
    /\ tAutoColumns \in BOOLEAN
    /\ tHasSource \in BOOLEAN
    /\ hasView \in BOOLEAN
    /\ vInclude \in Tri
    /\ vAutoColumns \in BOOLEAN
    /\ tAutoColumns => tHasSource
    /\ ~hasView => vInclude = "null" /\ vAutoColumns

Next == UNCHANGED vars

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ defaults \subseteq Flags
    /\ queueOverride \in [Flags -> Tri]

AutoViewMatchesAutoTable ==
    AutoViewExists =>
        /\ {m[2] : m \in AutoViewMappings} = TCols
        /\ \A m \in AutoViewMappings : m[1] \in KafkaSelectable

ExplicitViewConsistentWithTargetAndConfig ==
    hasView =>
        /\ \A m \in ViewMappings : m[1] \in KafkaSelectable
        /\ vInclude = "false" => ViewMetaTargets = {}
        /\ vInclude = "true" => ViewMetaTargets # {}

ExplicitViewTargetsExistingColumns ==
    hasView => \A m \in ViewMappings : m[2] \in TCols
=============================================================================
