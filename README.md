# spanner-migration
This project is just lightweight wrapper of migration commands to Google Spanner

You can download working jar from Releases

Before you run, make sure your machine is already authenticated with GCloud

For now, it only run Up Migrations, there is no way to rollback previous migration (Down Migration).

The migration file name must follow this naming convention: `{version/timestamp}_{migration_description}.yml`

Here is the format of migration file:
```
up:
  - [DB Operation: CreateTable | AddColumns | CreateIndex]
```

Here are format of supported DB Operations:

CreateTable

```
  - type: CreateTable
    tableName: [Table Name]
    columns:
      - type: [Column Type: Int64 | String | Timestamp | Bool]
        name: [Column Name]
        required: [Required Flag: true | false]
    primaryKeys:
      - name: [Column Name]
        order: [Order: desc | asc]
```

AddColumns
```
  - type: AddColumns
    tableName: [Table Name]
    columns:
      - type: [Column Type: Int64 | String | Timestamp | Bool]
        name: [Column Name]
        required: [Required Flag: true | false]
```

CreateIndex
```
  - type: CreateIndex
    indexName: [Index Name]
    tableName: [Table Name]
    indexColumns:
      - name: [Column Name]
        order: [Order: desc | asc]
    storedColumns:
      - [Column Name]

```

You can see some example files in `example` folder.


To run it

```bash
java -jar build/libs/spanner-migration-0.0.1-SNAPSHOT.jar \
--migration-dir ~/Workspace/spanner-migration/examples/migrate \
--project-id your-project \
--instance-id your-instance \
--database-id your-database
```
All inspiration is derived from Ruby on Rails' simple migration system: http://edgeguides.rubyonrails.org/active_record_migrations.html

kthxbye
