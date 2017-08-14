# spanner-migration
This project is just lightweight wrapper of migration commands to Google Spanner

You can download working jar from Releases

Example migration files with name `{timestamp}_create_status.yml` look like below:

```yaml
up:
  - type: CreateTable
    name: Status
    columns:
      - type: Int65
        name: userRegId
        required: true
      - type: Timestamp
        name: createdAt
        required: true
      - type: String
        name: uuid
        maxLength: 64
      - type: String
        name: newData
        maxLength: 160
    primaryKeys:
      - name: userRegId
      - name: createdAt
        order: desc
      - name: uuid
```

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
