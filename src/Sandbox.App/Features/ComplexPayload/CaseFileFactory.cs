using ProtobufTimestamp = Google.Protobuf.WellKnownTypes.Timestamp;

namespace Sandbox.App.Features.ComplexPayload;

public static class CaseFileFactory
{
    private static readonly string[] Cities = ["Berlin", "Lyon", "Kazan", "Oslo"];
    private static readonly string[] Countries = ["DE", "FR", "RU", "NO"];

    public static void Attach(OrderEvent orderEvent, DateTimeOffset now) =>
        Attach(orderEvent, now, Random.Shared);

    public static void Attach(OrderEvent orderEvent, DateTimeOffset now, Random random)
    {
        orderEvent.CaseFile = CreateBase(now, orderEvent.OrderId, random);
        PayloadSize.Fit(orderEvent, orderEvent.CaseFile);
    }

    public static void Attach(ShipmentEvent shipmentEvent, DateTimeOffset now) =>
        Attach(shipmentEvent, now, Random.Shared);

    public static void Attach(ShipmentEvent shipmentEvent, DateTimeOffset now, Random random)
    {
        shipmentEvent.CaseFile = CreateBase(now, shipmentEvent.ShipmentId, random);
        PayloadSize.Fit(shipmentEvent, shipmentEvent.CaseFile);
    }

    public static CaseFile CreateBase(DateTimeOffset now, string ownerId, Random random)
    {
        var file = new CaseFile
        {
            FileId = $"file-{ownerId}",
            Stage = CaseStage.Open,
            OpenedAt = ProtobufTimestamp.FromDateTimeOffset(now),
            Headline = $"dossier {ownerId}",
            Principal = CreateParty(random, "principal", 0),
            Evidence = new EvidenceBundle(),
            Seal = ByteString.CopyFrom(RandomBytes(random, 24)),
            ClerkNote = "filed-by-sandbox"
        };

        file.Labels["owner"] = ownerId;
        file.Labels["desk"] = "sandbox";
        file.Labels["priority"] = random.Next(1, 5).ToString();
        file.Markers.Add("origin");
        file.Markers.Add("complex");
        file.Markers.Add(ownerId[..Math.Min(8, ownerId.Length)]);

        for (var i = 0; i < 3; i++)
            file.Counterparties.Add(CreateParty(random, "counterparty", i + 1));

        for (var i = 0; i < 4; i++)
            file.Obligations.Add(CreateObligation(now, i));

        for (var i = 0; i < 3; i++)
            file.Evidence.Exhibits.Add(CreateExhibit(i));

        for (var i = 0; i < 2; i++)
            file.Evidence.Statements.Add(CreateStatement(now, i));

        for (var i = 0; i < 2; i++)
            file.Schedules.Add(CreateSchedule(now, i));

        ApplyInstrument(file, random);
        return file;
    }

    private static void ApplyInstrument(CaseFile file, Random random)
    {
        switch (random.Next(3))
        {
            case 0:
                file.Bank = new BankInstrument
                {
                    Iban = "DE89370400440532013000",
                    Bic = "COBADEFFXXX",
                    Holder = file.Principal.LegalName,
                    Branch = "central"
                };
                break;
            case 1:
                file.Wallet = new WalletInstrument
                {
                    Provider = "yard-pay",
                    AccountRef = $"wal-{random.Next(100000, 999999)}",
                    DisplayName = file.Principal.LegalName
                };
                break;
            default:
                var ledger = new LedgerInstrument
                {
                    Book = "trade",
                    Account = "4000-receivable",
                    PostedMinor = random.Next(10_000, 250_000)
                };
                ledger.Dimensions.Add("region-eu");
                ledger.Dimensions.Add("desk-sandbox");
                file.Ledger = ledger;
                break;
        }
    }

    private static Party CreateParty(Random random, string role, int index)
    {
        var party = new Party
        {
            PartyId = $"{role}-{index}",
            LegalName = $"{role} {index} llc",
            Residence = new Address
            {
                Country = Countries[index % Countries.Length],
                City = Cities[index % Cities.Length],
                Street = $"{index + 1} Harbor Lane",
                PostalCode = (10000 + index * 17).ToString()
            },
            RegistrationNo = $"REG-{1000 + index}"
        };
        party.Traits["role"] = role;
        party.Traits["index"] = index.ToString();
        party.Aliases.Add($"{role}-alias-{index}");
        party.Contacts.Add(new ContactPoint
        {
            Kind = ContactKind.Email,
            Value = $"{role}.{index}@example.test",
            Preferred = true,
            Note = "primary"
        });
        party.Contacts.Add(new ContactPoint
        {
            Kind = ContactKind.Phone,
            Value = $"+49000000{index:D4}",
            Preferred = false
        });
        if (random.Next(0, 2) == 0)
            party.Aliases.Add($"{role}-trade-{index}");

        return party;
    }

    private static Obligation CreateObligation(DateTimeOffset now, int index)
    {
        var obligation = new Obligation
        {
            ObligationId = $"obl-{index:D2}",
            Amount = new Money { Currency = index % 2 == 0 ? "EUR" : "USD", Amount = 100.5 + index },
            DueAt = ProtobufTimestamp.FromDateTimeOffset(now.AddDays(index + 1)),
            Memo = $"memo-{index}:{CaseFileText.Create(200 + index, 180)}"
        };
        obligation.Covenants.Add("notice");
        obligation.Covenants.Add(index % 2 == 0 ? "insurance" : "inspection");
        for (var step = 0; step < 3; step++)
        {
            var installment = new Installment
            {
                Sequence = (uint)(step + 1),
                Amount = new Money { Currency = obligation.Amount.Currency, Amount = 25.25 + step },
                DueAt = ProtobufTimestamp.FromDateTimeOffset(now.AddDays(index + step + 1))
            };
            installment.Notes.Add($"installment-{index}-{step}");
            installment.Notes.Add(CaseFileText.Create(300 + index * 10 + step, 80));
            obligation.Installments.Add(installment);
        }

        return obligation;
    }

    private static Exhibit CreateExhibit(int index)
    {
        var exhibit = new Exhibit
        {
            ExhibitId = $"exhibit-{index:D2}",
            Title = $"exhibit {index}",
            Digest = ByteString.CopyFrom(Enumerable.Range(0, 16).Select(i => (byte)(index * 13 + i)).ToArray())
        };
        exhibit.Attributes["class"] = index % 2 == 0 ? "contract" : "photo";
        exhibit.Attributes["index"] = index.ToString();
        for (var page = 1; page <= 4; page++)
        {
            var excerpt = new Excerpt
            {
                Page = (uint)page,
                Text = $"excerpt-{index}-{page}|{CaseFileText.Create(400 + index * 20 + page, 1500)}",
                Annotation = page % 2 == 0 ? $"ann-{index}-{page}" : null
            };
            excerpt.Highlights.Add($"h-{index}-{page}");
            exhibit.Excerpts.Add(excerpt);
        }

        return exhibit;
    }

    private static Statement CreateStatement(DateTimeOffset now, int index)
    {
        var statement = new Statement
        {
            Author = index == 0 ? "auditor" : "carrier",
            StatedAt = ProtobufTimestamp.FromDateTimeOffset(now.AddHours(-index)),
            Body = $"statement-{index}|{CaseFileText.Create(500 + index, 1200)}"
        };
        statement.Witnesses.Add("witness-a");
        statement.Witnesses.Add($"witness-{index}");
        return statement;
    }

    private static Schedule CreateSchedule(DateTimeOffset now, int index)
    {
        var schedule = new Schedule
        {
            ScheduleId = $"schedule-{index}",
            Title = $"window-plan-{index}"
        };
        for (var windowIndex = 0; windowIndex < 2; windowIndex++)
        {
            var window = new Window
            {
                WindowId = $"window-{index}-{windowIndex}",
                StartsAt = ProtobufTimestamp.FromDateTimeOffset(now.AddHours(windowIndex + 1))
            };
            for (var slotIndex = 0; slotIndex < 3; slotIndex++)
            {
                var slot = new Slot
                {
                    Index = (uint)slotIndex,
                    Label = $"slot-{index}-{windowIndex}-{slotIndex}",
                    Note = $"note-{CaseFileText.Create(600 + index * 30 + windowIndex * 5 + slotIndex, 240)}"
                };
                slot.Assignees.Add(slotIndex % 2 == 0 ? "clerk" : "reviewer");
                slot.Assignees.Add($"desk-{index}");
                window.Slots.Add(slot);
            }

            schedule.Windows.Add(window);
        }

        return schedule;
    }

    private static byte[] RandomBytes(Random random, int length)
    {
        var bytes = new byte[length];
        random.NextBytes(bytes);
        return bytes;
    }
}
