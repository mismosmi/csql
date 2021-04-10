Composable SQL queries.

Use a regular postgres client (or pool)
```ts
const client = new pg.Client() // new pg.Pool()
```

Use queries like regular functions
```ts
const query = sql`SELECT name FROM users WHERE id = ${a`id`}`.query(client)

const names = ids.map(id => query({ id })?.rows?.[0]?.name)
```

Compose queries of fragments
```ts
const table = sql`SELECT 'test' AS name`

const query = sql`SELECT name FROM (${table})`.query(client)
query() // [{ name: 'test' }]
```

Properly escape literals, identifiers, values
```ts
const query = sql`SELECT ${i`myColumn`}, ${15} as boundVariable, ${l`literal`} as literalValue FROM ${i`myTable`}`
```

Parametrize Queries, map arguments
```ts
const idIs = sql`id = ${a`id`}`
const updateName = sql<{
    firstName: string
    lastName: string
    userId: string
}>`UPDATE SET name = ${args => `${args.firstName} ${args.lastName}`} WHERE ${idIs(args => ({ id: args.userId }))}`.query(client)

updateName({
    userId: '1337',
    firstName: "Maria",
    lastName: "Testova",
})
```
