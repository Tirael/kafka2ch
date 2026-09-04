FROM --platform=linux/amd64 mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY kafka2ch.sln ./
COPY src/Sandbox.Contracts/Sandbox.Contracts.csproj src/Sandbox.Contracts/
COPY src/Sandbox.App/Sandbox.App.csproj src/Sandbox.App/
RUN dotnet restore src/Sandbox.App/Sandbox.App.csproj

COPY src/ src/
RUN dotnet publish src/Sandbox.App/Sandbox.App.csproj -c Release -o /app/publish --no-restore

FROM --platform=linux/amd64 mcr.microsoft.com/dotnet/runtime:8.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "Sandbox.App.dll"]
