--------------------------- MODULE MigrationDiffDrift ---------------------------
(***************************************************************************)
(* Agreement between `build` (SnapshotDriftChecker) and `migrate`          *)
(* (SchemaSnapshotMapper.ToPlan -> SchemaDiffer -> MigrationPolicy ->      *)
(* MigrationSqlGenerator) for one pipeline: Kafka table q1, optional q2,   *)
(* MergeTree table t (source q1, columns id + c) and one MV into t.        *)
(*                                                                         *)
(* The old side is the committed snapshot; the new side is the old one     *)
(* with up to MaxChanges edits from Edits applied to the config.           *)
(* `build` fails on any drift and tells the user to run `migrate`, so      *)
(* every drift must yield a non-empty diff (DriftImpliesDiff).             *)
(*                                                                         *)
(* Impl = "buggy" is the code before PR #57 (defects 2.1-2.4),             *)
(* Impl = "fixed" is the current code.                                     *)
(***************************************************************************)
EXTENDS FiniteSets, Naturals

CONSTANTS Impl, MaxChanges, Edits

ASSUME Impl \in {"buggy", "fixed"}

AllEdits == {"addQueue", "removeQueue", "kafkaSetting", "persistMeta",
             "tSource", "tOrigin", "vOrigin", "renameCol", "retypeCol"}

ASSUME Edits \subseteq AllEdits

VARIABLES oHasQ2, oMeta, oTAuto, oVAuto, oOrderBy, edits

vars == <<oHasQ2, oMeta, oTAuto, oVAuto, oOrderBy, edits>>

Toggled(e, v) == IF e \in edits THEN ~v ELSE v

Side(isNew) ==
    LET hasQ2 == IF ~isNew THEN oHasQ2
                 ELSE IF "addQueue" \in edits THEN TRUE
                 ELSE IF "removeQueue" \in edits THEN FALSE
                 ELSE oHasQ2
        tAuto == IF isNew THEN Toggled("tOrigin", oTAuto) ELSE oTAuto
        vAuto == IF isNew THEN Toggled("vOrigin", oVAuto) ELSE oVAuto
        meta == IF isNew THEN Toggled("persistMeta", oMeta) ELSE oMeta
        setting == IF isNew /\ "kafkaSetting" \in edits THEN 1 ELSE 0
        tSource == IF isNew /\ "tSource" \in edits THEN "q2" ELSE "q1"
        colName == IF isNew /\ "renameCol" \in edits THEN "c2" ELSE "c1"
        colType == IF isNew /\ "retypeCol" \in edits THEN "Int64" ELSE "Int32"
        metaCols == IF tAuto /\ tSource = "q1" /\ meta
                    THEN {[key |-> "kafka:_key", name |-> "kafka_key", type |-> "String"]}
                    ELSE {}
        columns == {[key |-> "1", name |-> "id", type |-> "UInt64"],
                    [key |-> "2", name |-> colName, type |-> colType]} \cup metaCols
        mappings == IF vAuto
                    THEN {<<IF c.name = "kafka_key" THEN "_key" ELSE c.name, c.name>> : c \in columns}
                    ELSE {<<"id", "id">>, <<colName, colName>>}
    IN [queues |-> IF hasQ2 THEN {"q1", "q2"} ELSE {"q1"},
        setting |-> setting,
        meta |-> meta,
        tAuto |-> tAuto,
        tSource |-> tSource,
        orderBy |-> oOrderBy,
        orderByText |-> IF oOrderBy = "id" THEN "id"
                        ELSE IF oOrderBy = "plain" THEN colName
                        ELSE <<"toStartOfHour", colName>>,
        colName |-> colName,
        colType |-> colType,
        columns |-> columns,
        vAuto |-> vAuto,
        vSource |-> IF vAuto THEN tSource ELSE "q1",
        mappings |-> mappings]

When(cond, tag) == IF cond THEN {tag} ELSE {}

Old == Side(FALSE)
New == Side(TRUE)

Drift ==
    When(Old.queues # New.queues, "queues")
    \cup When(Old.setting # New.setting, "kafkaSetting")
    \cup When(Old.meta # New.meta, "persistMeta")
    \cup When(Old.tAuto # New.tAuto, "tOrigin")
    \cup When(Old.tSource # New.tSource, "tSource")
    \cup When(Old.orderByText # New.orderByText, "orderBy")
    \cup When(Old.columns # New.columns, "tColumns")
    \cup When(Old.vAuto # New.vAuto, "vOrigin")
    \cup When(Old.vSource # New.vSource, "vSource")
    \cup When(Old.mappings # New.mappings, "vMappings")

ToPlanSucceeds == Impl = "fixed" \/ Old.queues \subseteq New.queues

ColumnRenamed == Old.colName # New.colName
ColumnRetyped == Old.colType # New.colType

ParsedOrderBy(side) ==
    IF side.orderBy = "id" THEN {"id"}
    ELSE IF side.orderBy = "plain" \/ Impl = "fixed" THEN {side.colName}
    ELSE {}

OrderByColumns ==
    IF Impl = "fixed" THEN ParsedOrderBy(Old) \cup ParsedOrderBy(New)
    ELSE ParsedOrderBy(New)

RetypeKindManual ==
    ColumnRetyped /\
      IF Impl = "fixed"
      THEN Old.colName \in OrderByColumns \/ New.colName \in OrderByColumns
      ELSE Old.colName \in OrderByColumns

\* DiffKafkaDependentViews (#73): persistKafkaMeta-only change → ViewChanged when
\* an MV sources the queue (this model always has one MV into t).
ViewChanged ==
    \/ Impl = "fixed" /\ Old.vAuto # New.vAuto
    \/ Impl = "fixed" /\ Old.meta # New.meta
    \/ Old.vSource # New.vSource
    \/ Old.mappings # New.mappings

Diff ==
    When(New.queues \ Old.queues # {}, "kafkaAdded")
    \cup When(Old.queues \ New.queues # {}, "kafkaRemoved")
    \cup When(Old.setting # New.setting, "kafkaChanged")
    \cup When(Old.tAuto # New.tAuto, "originChanged")
    \cup When(Impl = "fixed" /\ Old.tSource # New.tSource, "sourceTableChanged")
    \cup When(Old.orderByText # New.orderByText, "orderByChanged")
    \cup When(ColumnRenamed, "columnRenamed")
    \cup When(ColumnRetyped, "columnTypeChanged")
    \cup When(\E c \in New.columns : c.key \notin {o.key : o \in Old.columns}, "columnAdded")
    \cup When(\E c \in Old.columns : c.key \notin {n.key : n \in New.columns}, "columnRemoved")
    \cup When(ViewChanged, "viewChanged")

TouchesOrderByColumn ==
    \/ ColumnRenamed /\ (IF Impl = "fixed"
                         THEN Old.colName \in OrderByColumns \/ New.colName \in OrderByColumns
                         ELSE Old.colName \in OrderByColumns)
    \/ ColumnRetyped /\ Old.colName \in OrderByColumns

TableIsManual ==
    \/ "originChanged" \in Diff
    \/ "orderByChanged" \in Diff
    \/ RetypeKindManual
    \/ Impl = "fixed" /\ TouchesOrderByColumn

MigrationCreatesQueues ==
    IF Impl = "fixed" THEN New.queues \ Old.queues ELSE {}

Init ==
    /\ oHasQ2 \in BOOLEAN
    /\ oMeta \in BOOLEAN
    /\ oTAuto \in BOOLEAN
    /\ oVAuto \in BOOLEAN
    /\ oOrderBy \in {"id", "plain", "wrapped"}
    /\ edits \in {s \in SUBSET Edits : Cardinality(s) <= MaxChanges}
    /\ "addQueue" \in edits => ~oHasQ2
    /\ "removeQueue" \in edits => oHasQ2
    /\ "tSource" \in edits => (oHasQ2 /\ "removeQueue" \notin edits) \/ "addQueue" \in edits
    /\ oVAuto => oTAuto
    /\ Toggled("vOrigin", oVAuto) => Toggled("tOrigin", oTAuto)

Next == UNCHANGED vars

Spec == Init /\ [][Next]_vars

DriftImpliesDiff ==
    Drift # {} => ToPlanSucceeds /\ Diff # {}

CanMigrateRemovedOrAddedKafkaTable ==
    Old.queues # New.queues =>
        /\ ToPlanSucceeds
        /\ New.queues \ Old.queues # {} => "kafkaAdded" \in Diff
        /\ Old.queues \ New.queues # {} => "kafkaRemoved" \in Diff

AddedKafkaTableIsCreatedInMigration ==
    "kafkaAdded" \in Diff => New.queues \ Old.queues \subseteq MigrationCreatesQueues

OrderByRenameIsManual ==
    (ColumnRenamed \/ ColumnRetyped) /\ Old.orderBy # "id" => TableIsManual
=============================================================================
