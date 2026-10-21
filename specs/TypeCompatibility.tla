---------------------------- MODULE TypeCompatibility ----------------------------
(***************************************************************************)
(* TypeCompatibility.Classify for integer column types and their           *)
(* Nullable(...) wrappers. Safe means the migrator runs                    *)
(* ALTER TABLE ... MODIFY COLUMN automatically, so it must never lose      *)
(* values.                                                                 *)
(*                                                                         *)
(* Widths 8/16/32/64 are scaled to Scale[w] bits so TLC can enumerate the  *)
(* value ranges; containment between ranges is preserved by the scaling.   *)
(*                                                                         *)
(* Impl = "buggy" is the single rank Int8 < ... < Int64 < UInt8 < ...      *)
(* < UInt64 before PR #57 (defect 3.2), Impl = "fixed" is the current      *)
(* (!old.Signed || new.Signed) && new.Bits > old.Bits rule.                *)
(***************************************************************************)
EXTENDS Integers

CONSTANTS Impl

ASSUME Impl \in {"buggy", "fixed"}

Widths == {8, 16, 32, 64}
Scale == [w \in Widths |-> CASE w = 8 -> 2 [] w = 16 -> 3 [] w = 32 -> 4 [] w = 64 -> 5]

IntTypes == [signed : BOOLEAN, bits : Widths]
Types == [base : IntTypes, nullable : BOOLEAN]

NullValue == -1000

BaseRange(t) ==
    LET b == Scale[t.bits]
    IN IF t.signed THEN -(2^(b - 1)) .. 2^(b - 1) - 1 ELSE 0 .. 2^b - 1

Values(t) == BaseRange(t.base) \cup (IF t.nullable THEN {NullValue} ELSE {})

Lossless(o, n) == Values(o) \subseteq Values(n)

Rank(t) == (IF t.signed THEN 0 ELSE 4) +
           (CASE t.bits = 8 -> 1 [] t.bits = 16 -> 2 [] t.bits = 32 -> 3 [] t.bits = 64 -> 4)

IsIntegerWidening(o, n) ==
    /\ ~o.nullable /\ ~n.nullable
    /\ IF Impl = "buggy"
       THEN Rank(n.base) > Rank(o.base)
       ELSE (~o.base.signed \/ n.base.signed) /\ n.base.bits > o.base.bits

Classify(o, n) ==
    IF o = n THEN "Safe"
    ELSE IF o.nullable /\ ~n.nullable THEN "Destructive"
    ELSE IF ~o.nullable /\ n.nullable /\ n.base = o.base THEN "Safe"
    ELSE IF IsIntegerWidening(o, n) THEN "Safe"
    ELSE "Manual"

VARIABLES old, new

vars == <<old, new>>

Init == old \in Types /\ new \in Types

Next == UNCHANGED vars

Spec == Init /\ [][Next]_vars

SafeWideningIsLossless ==
    Classify(old, new) = "Safe" => Lossless(old, new)

LosslessIntegerWideningIsSafe ==
    (~old.nullable /\ ~new.nullable /\ Lossless(old, new)) => Classify(old, new) = "Safe"
=============================================================================
