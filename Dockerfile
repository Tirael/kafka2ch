FROM --platform=linux/amd64 mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY kafka2ch.sln ./
COPY src/Sandbox.Contracts/Sandbox.Contracts.csproj src/Sandbox.Contracts/
COPY src/Sandbox.App/Sandbox.App.csproj src/Sandbox.App/
COPY tools/ClickHouseSchemaGen/ClickHouseSchemaGen.csproj tools/ClickHouseSchemaGen/
COPY tools/ClickHouseSchemaGen.Migrator/ClickHouseSchemaGen.Migrator.csproj tools/ClickHouseSchemaGen.Migrator/
RUN dotnet restore src/Sandbox.App/Sandbox.App.csproj
RUN dotnet restore tools/ClickHouseSchemaGen.Migrator/ClickHouseSchemaGen.Migrator.csproj

COPY src/ src/
COPY tools/ tools/
RUN dotnet publish src/Sandbox.App/Sandbox.App.csproj -c Release -o /app/publish --no-restore -p:SkipClickHouseCodegen=true
RUN dotnet publish tools/ClickHouseSchemaGen.Migrator/ClickHouseSchemaGen.Migrator.csproj -c Release -o /app/migrator --no-restore -p:SkipClickHouseCodegen=true

FROM --platform=linux/amd64 mcr.microsoft.com/dotnet/runtime:8.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "Sandbox.App.dll"]

FROM --platform=linux/amd64 mcr.microsoft.com/dotnet/runtime:8.0 AS migrator
WORKDIR /app
COPY --from=build /app/migrator .
ENTRYPOINT ["dotnet", "ClickHouseSchemaGen.Migrator.dll", "--migrations", "/migrations"]
