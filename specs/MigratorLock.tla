------------------------------ MODULE MigratorLock ------------------------------
(***************************************************************************)
(* Two (or more) Migrator processes apply the same pending migration to a  *)
(* cluster. Its DDL runs ON CLUSTER, i.e. it lands on each node            *)
(* separately and may fail after some nodes have executed it.              *)
(*                                                                         *)
(* Code: MigrationRunner.ApplyAsync and MigrationLock (W2-1, PR #69).      *)
(*                                                                         *)
(* Variant = "unlocked": main before W2-1. No lock; LoadApplied, apply,    *)
(*   record a `migration` row.                                             *)
(* Variant = "locked": W2-1. KeeperMap lock row inserted with              *)
(*   keeper_map_strict_mode = 1 before LoadApplied, lease                  *)
(*   expires_at = now + TTL, expired holder deleted by owner and the       *)
(*   insert retried, `started` row before the DDL and `migration` row      *)
(*   after it, release in `finally` (DELETE ... WHERE owner = self).       *)
(*   The lease is never renewed. The owner string contains a per-process   *)
(*   GUID, so each run is a distinct owner <<m, run>>.                     *)
(*                                                                         *)
(* LeaseOutlivesMigration = TRUE restricts time so that a live holder      *)
(* never runs past its lease (the documented operating assumption          *)
(* `--lock-ttl` > longest migration). FALSE drops it.                      *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Migrators, Nodes, Variant, TTL, MaxTime, MaxRuns,
          LeaseOutlivesMigration

ASSUME Variant \in {"unlocked", "locked"}
ASSUME TTL \in Nat \ {0} /\ MaxTime \in Nat /\ MaxRuns \in Nat \ {0}
ASSUME LeaseOutlivesMigration \in BOOLEAN

None == <<"none", 0>>

VARIABLES
    pc,
    clock,
    lock,
    seenHolder,
    pending,
    warned,
    runs,
    startedRows,
    migrationRows,
    executed,
    interrupted,
    silentReapply

vars == <<pc, clock, lock, seenHolder, pending, warned, runs, startedRows,
          migrationRows, executed, interrupted, silentReapply>>

Locked == Variant = "locked"

Holding == {"loading", "starting", "applying", "recording", "releasing"}
Critical == {"starting", "applying", "recording"}

Owner(m) == <<m, runs[m]>>

Expired == clock >= lock.expires

LiveHolderAtLeaseEnd ==
    \E m \in Migrators :
        /\ lock.owner = Owner(m)
        /\ pc[m] \in Holding
        /\ clock + 1 >= lock.expires

Init ==
    /\ pc = [m \in Migrators |-> "acquiring"]
    /\ clock = 0
    /\ lock = [owner |-> None, expires |-> 0]
    /\ seenHolder = [m \in Migrators |-> None]
    /\ pending = [m \in Migrators |-> {}]
    /\ warned = [m \in Migrators |-> FALSE]
    /\ runs = [m \in Migrators |-> 1]
    /\ startedRows = 0
    /\ migrationRows = 0
    /\ executed = [n \in Nodes |-> 0]
    /\ interrupted = 0
    /\ silentReapply = FALSE

Acquire(m) ==
    /\ pc[m] = "acquiring"
    /\ IF ~Locked
       THEN /\ pc' = [pc EXCEPT ![m] = "loading"]
            /\ UNCHANGED <<lock, seenHolder>>
       ELSE IF lock.owner = None
       THEN /\ lock' = [owner |-> Owner(m), expires |-> clock + TTL]
            /\ pc' = [pc EXCEPT ![m] = "loading"]
            /\ UNCHANGED seenHolder
       ELSE /\ seenHolder' = [seenHolder EXCEPT ![m] = lock.owner]
            /\ pc' = [pc EXCEPT ![m] = IF Expired THEN "takingOver" ELSE "waiting"]
            /\ UNCHANGED lock
    /\ UNCHANGED <<clock, pending, warned, runs, startedRows, migrationRows,
                   executed, interrupted, silentReapply>>

TakeOver(m) ==
    /\ pc[m] = "takingOver"
    /\ lock' = IF lock.owner = seenHolder[m]
               THEN [owner |-> None, expires |-> 0]
               ELSE lock
    /\ pc' = [pc EXCEPT ![m] = "acquiring"]
    /\ UNCHANGED <<clock, seenHolder, pending, warned, runs, startedRows,
                   migrationRows, executed, interrupted, silentReapply>>

Retry(m) ==
    /\ pc[m] = "waiting"
    /\ pc' = [pc EXCEPT ![m] = "acquiring"]
    /\ UNCHANGED <<clock, lock, seenHolder, pending, warned, runs, startedRows,
                   migrationRows, executed, interrupted, silentReapply>>

Load(m) ==
    /\ pc[m] = "loading"
    /\ warned' = [warned EXCEPT ![m] = Locked /\ startedRows > migrationRows]
    /\ pc' = [pc EXCEPT ![m] =
                IF migrationRows > 0 THEN "releasing"
                ELSE IF Locked THEN "starting"
                ELSE "applying"]
    /\ pending' = [pending EXCEPT ![m] =
                    IF migrationRows = 0 /\ ~Locked THEN Nodes ELSE {}]
    /\ UNCHANGED <<clock, lock, seenHolder, runs, startedRows, migrationRows,
                   executed, interrupted, silentReapply>>

RecordStarted(m) ==
    /\ pc[m] = "starting"
    /\ startedRows' = startedRows + 1
    /\ pending' = [pending EXCEPT ![m] = Nodes]
    /\ pc' = [pc EXCEPT ![m] = "applying"]
    /\ UNCHANGED <<clock, lock, seenHolder, warned, runs, migrationRows,
                   executed, interrupted, silentReapply>>

ExecuteOn(m, n) ==
    /\ pc[m] = "applying"
    /\ n \in pending[m]
    /\ executed' = [executed EXCEPT ![n] = @ + 1]
    /\ silentReapply' = (silentReapply \/ (executed[n] > 0 /\ ~warned[m]))
    /\ pending' = [pending EXCEPT ![m] = @ \ {n}]
    /\ UNCHANGED <<pc, clock, lock, seenHolder, warned, runs, startedRows,
                   migrationRows, interrupted>>

FailOnCluster(m) ==
    /\ pc[m] = "applying"
    /\ pending[m] # {}
    /\ pending[m] # Nodes
    /\ interrupted' = interrupted + 1
    /\ pending' = [pending EXCEPT ![m] = {}]
    /\ pc' = [pc EXCEPT ![m] = "releasing"]
    /\ UNCHANGED <<clock, lock, seenHolder, warned, runs, startedRows,
                   migrationRows, executed, silentReapply>>

FinishDdl(m) ==
    /\ pc[m] = "applying"
    /\ pending[m] = {}
    /\ pc' = [pc EXCEPT ![m] = "recording"]
    /\ UNCHANGED <<clock, lock, seenHolder, pending, warned, runs, startedRows,
                   migrationRows, executed, interrupted, silentReapply>>

RecordMigration(m) ==
    /\ pc[m] = "recording"
    /\ migrationRows' = migrationRows + 1
    /\ pc' = [pc EXCEPT ![m] = "releasing"]
    /\ UNCHANGED <<clock, lock, seenHolder, pending, warned, runs, startedRows,
                   executed, interrupted, silentReapply>>

Release(m) ==
    /\ pc[m] = "releasing"
    /\ lock' = IF Locked /\ lock.owner = Owner(m)
               THEN [owner |-> None, expires |-> 0]
               ELSE lock
    /\ pc' = [pc EXCEPT ![m] = "done"]
    /\ UNCHANGED <<clock, seenHolder, pending, warned, runs, startedRows,
                   migrationRows, executed, interrupted, silentReapply>>

Crash(m) ==
    /\ pc[m] \in Holding
    /\ interrupted' = IF pc[m] \in Critical THEN interrupted + 1 ELSE interrupted
    /\ pending' = [pending EXCEPT ![m] = {}]
    /\ pc' = [pc EXCEPT ![m] = "crashed"]
    /\ UNCHANGED <<clock, lock, seenHolder, warned, runs, startedRows,
                   migrationRows, executed, silentReapply>>

Rerun(m) ==
    /\ pc[m] \in {"done", "crashed"}
    /\ runs[m] < MaxRuns
    /\ runs' = [runs EXCEPT ![m] = @ + 1]
    /\ pc' = [pc EXCEPT ![m] = "acquiring"]
    /\ warned' = [warned EXCEPT ![m] = FALSE]
    /\ UNCHANGED <<clock, lock, seenHolder, pending, startedRows,
                   migrationRows, executed, interrupted, silentReapply>>

Tick ==
    /\ clock < MaxTime
    /\ LeaseOutlivesMigration => ~LiveHolderAtLeaseEnd
    /\ clock' = clock + 1
    /\ UNCHANGED <<pc, lock, seenHolder, pending, warned, runs, startedRows,
                   migrationRows, executed, interrupted, silentReapply>>

Next ==
    \/ Tick
    \/ \E m \in Migrators :
        \/ Acquire(m) \/ TakeOver(m) \/ Retry(m) \/ Load(m)
        \/ RecordStarted(m) \/ FailOnCluster(m) \/ FinishDdl(m)
        \/ RecordMigration(m) \/ Release(m) \/ Crash(m) \/ Rerun(m)
        \/ \E n \in Nodes : ExecuteOn(m, n)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ pc \in [Migrators -> {"acquiring", "waiting", "takingOver", "done",
                             "crashed"} \cup Holding]
    /\ lock.owner \in (Migrators \X (1..MaxRuns)) \cup {None}
    /\ pending \in [Migrators -> SUBSET Nodes]
    /\ executed \in [Nodes -> Nat]

MutualExclusion ==
    \A m1, m2 \in Migrators :
        m1 # m2 => ~(pc[m1] \in Critical /\ pc[m2] \in Critical)

NoDoubleApply ==
    \A n \in Nodes : executed[n] <= 1 + interrupted

ReapplyOnlyAfterInterruptedWarning == ~silentReapply

HistoryRecordedOnce == migrationRows <= 1

PartialApplyIsJournaled ==
    ((\E n \in Nodes : executed[n] > 0) /\ migrationRows = 0) => startedRows > 0

RecordedImpliesAppliedEverywhere ==
    migrationRows > 0 => \A n \in Nodes : executed[n] > 0

EachNodeExecutesOnce == \A n \in Nodes : executed[n] <= 1

=============================================================================
