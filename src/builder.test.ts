import { sql, i, a, l } from "./index";

describe("query builder", () => {
  it("can build a query", () => {
    const query = sql`SELECT 1`;
    expect(query().toString()).toBe("SELECT 1");
  });

  it("can accept values", () => {
    const queryA = sql`SELECT ${3}`;
    expect(queryA().toString()).toBe("SELECT <VALUE 3>");
    const queryB = sql`SELECT ${null}`;
    expect(queryB().toString()).toBe("SELECT <VALUE null>");
    const queryC = sql`SELECT ${{ test: "TEST" }}`;
    expect(queryC().toString()).toBe('SELECT <VALUE {"test":"TEST"}>');
  });

  it("can accept subqueries", () => {
    const inner = sql`SELECT 1 AS value`;
    const outer = sql`SELECT value FROM (${inner})`;
    expect(outer().toString()).toBe("SELECT value FROM (SELECT 1 AS value)");
  });

  it("can accept arguments", () => {
    const query = sql<{ value: number }>`SELECT ${(props) => props.value}`;
    expect(query().toString()).toBe(
      "SELECT <ARG function (props) { return props.value; }>"
    );
  });

  it("can accept identifiers", () => {
    const queryA = sql`QUERY ${i`test`}`;
    expect(queryA().toString()).toBe("QUERY <IDENT test>");
    const queryB = sql`QUERY ${i("test")}`;
    expect(queryB().toString()).toBe("QUERY <IDENT test>");
  });

  it("can accept literals", () => {
    const queryA = sql`QUERY ${l`test`}`;
    expect(queryA().toString()).toBe('QUERY <LITERAL "test">');
    const queryB = sql`QUERY ${l("test")}`;
    expect(queryB().toString()).toBe('QUERY <LITERAL "test">');
    const queryC = sql`QUERY ${l(3)}`;
    expect(queryC().toString()).toBe("QUERY <LITERAL 3>");
    const queryD = sql`QUERY ${l(null)}`;
    expect(queryD().toString()).toBe("QUERY <LITERAL null>");
  });

  it("can accept argument shorthand", () => {
    const queryA = sql<{ value: 3 }>`QUERY ${a`value`}`;
    expect(queryA().toString()).toBe(
      "QUERY <ARG function (args) { return args[name[0]]; }>"
    );
    const queryB = sql<{ value: 3 }>`QUERY ${a("value")}`;
    expect(queryB().toString()).toBe(
      "QUERY <ARG function (args) { return args[name]; }>"
    );
  });
});
