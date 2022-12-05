## Quickstart - Custom Views
This project contains an example using the custom views library to project ledger events to a PostgreSQL table. If you 
are not familiar with IOUs, please read the [IOU overview](https://docs.daml.com/app-dev/bindings-java/quickstart.html#tutorials-iou-overview).

The example provides a script in `ProjectionRunner.java` to start the projection. It also provides a spring-boot
application with a REST API over the projected events in the database.

Note: Before starting, please ensure you have the Daml SDK installed. You can find instructions on how to do that [here](https://docs.daml.com/getting-started/installation.html#installing-the-sdk)

# Prerequisite

 - Maven (https://maven.apache.org/install.html)

 - PostgreSql Client
   
   - MacOS with Homebrew (https://formulae.brew.sh/formula/libpq)

   - Windows (https://www.postgresql.org/download/windows/)

 - Docker (https://docs.docker.com/get-docker/)

# How to run

In the quickstart directory, compile Daml model with: 

    daml build

Startup a sandbox ledger in a separate terminal with: 

    daml sandbox --dar .daml/dist/quickstart-0.0.1.dar

Generate java code from the daml source with: 

    daml codegen java

Start Navigator with: 

    daml navigator server

Start a PostgreSQL database with docker compose:

    docker-compose up -d db

Create a new database named `ious` in the Postgres db.

    psql -h localhost -p 5432 -U postgres -c 'create database ious'

Initialise ledger with some parties with the following command:

    daml script --dar .daml/dist/quickstart-0.0.1.dar --script-name Main:initialize --ledger-host localhost --ledger-port 6865 --static-time --output-file parties.json

This will print something like the following:

    [DA.Internal.Prelude:556]: 'Alice::1220c7b4b153d8dec59ceb424bb700f2c8032ec48a13195f580c8fb099ff0ea196fc'

Export the party Id, in this case `Alice::1220c7b4b153d8dec59ceb424bb700f2c8032ec48a13195f580c8fb099ff0ea196fc`:

    export PARTY_ID="Alice::1220c7b4b153d8dec59ceb424bb700f2c8032ec48a13195f580c8fb099ff0ea196fc"

Run the migration to create the tables with the following command:

    mvn flyway:migrate -Dflyway.configFiles=src/main/resources/flyway.conf

Start the REST API with the following command:

    mvn spring-boot:run

Start the projection runner with the following command:

    mvn exec:java -Dexec.mainClass="com.daml.quickstart.iou.ProjectionRunner" -Dexec.args="$PARTY_ID"

You should be able to see events projected to the database by checking the database directly or using one of the APIs below.
- `GET /events`
- `GET /events/count`
