import { sql } from "./index";
import { Client } from "pg";
import { mocked } from "ts-jest";

describe("query", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });
  const client = {
    query: jest.fn(),
  };

  it("can run a simple query", () => {
    const query = sql`QUERY`.query(client);
    query();
    expect(client.query).toHaveBeenCalledWith({
      name: expect.any(String),
      text: "QUERY",
      values: [],
    });
  });

  it("can run a query with a value", () => {
    const query = sql`QUERY ${3}`.query(client);
    query();
    expect(client.query).toHaveBeenCalledWith({
      name: expect.any(String),
      text: "QUERY $1",
      values: [3],
    });
  });

  it("can run a query with arguments", () => {
    const query = sql<{ value: number }>`QUERY ${(args) => args.value}`.query(
      client
    );
    query({ value: 3 });
    expect(client.query).toHaveBeenCalledWith({
      name: expect.any(String),
      text: "QUERY $1",
      values: [3],
    });
  });

  it("correctly assigns parameters to nested values", () => {
    const inner = sql`INNER ${1} ${2}`;
    sql`OUTER (${inner}) ${3}`.query(client)();
    expect(client.query).toHaveBeenCalledWith({
      name: expect.any(String),
      text: "OUTER (INNER $1 $2) $3",
      values: [1, 2, 3],
    });

    client.query.mockClear();
    sql`OUTER ${3} (${inner})`.query(client)();
    expect(client.query).toHaveBeenCalledWith({
      name: expect.any(String),
      text: "OUTER $1 (INNER $2 $3)",
      values: [3, 1, 2],
    });
  });

  it("utilizes query caching", () => {
    const query = sql`QUERY`.query(client);

    query();
    expect(client.query).toHaveBeenCalledTimes(1);
    const name = client.query.mock.calls[0][0].name;
    query();
    expect(client.query).toHaveBeenCalledTimes(2);
    expect(client.query.mock.calls[1][0].name).toBe(name);
  });
});
