import { sql } from "./index";

describe("csql", () => {
  it("can build a query", () => {
    const query = sql`SELECT 1`;
    expect(query().toString()).toBe("SELECT 1");
  });
});
