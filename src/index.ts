import { Pool, Client, QueryResultRow } from "pg";
import format from "pg-format";
import * as uuid from "uuid";

type Value =
  | null
  | undefined
  | string
  | number
  | Date
  | Buffer
  | Value[]
  | { [key: string]: Value };

type Accessor<A> = (values: A) => Value;

type Mapper<I, A> = (values: I) => A;

type QueryResult<Row> = {
  rows: Row[];
};

interface QueryMethod<A> {
  <Row extends QueryResultRow = any>(conn: Pool | Client, values?: A):
    | ((values: A) => Promise<QueryResult<Row>>)
    | Promise<QueryResult<Row>>;
}

const isBuildMethodField = Symbol("isBuildMethod");
interface BuildMethod<A> {
  (): QueryFragment<A>;
  <I>(mapper: Mapper<I, A>): QueryFragment<I>;
  query: QueryMethod<A>;
  [isBuildMethodField]: true;
}

function isBuildMethod<A>(fn: Function): fn is BuildMethod<A> {
  return isBuildMethodField in fn && fn[isBuildMethodField];
}

abstract class Fragment<A> {
  abstract map<I>(mapper: Mapper<I, A>): Fragment<I>;
  abstract prepare(argOffset: number): [string, Accessor<A>[]];
  abstract toString(): string;

  public static from<A>(
    source: Value | BuildMethod<A> | Accessor<A> | Fragment<A>
  ): Fragment<A> {
    if (source instanceof Fragment) {
      return source;
    }

    if (typeof source === "function") {
      if (isBuildMethod<A>(source)) {
        return source();
      }

      return new ArgumentFragment(source);
    }

    return new ArgumentFragment(() => source);
  }
}

class StringFragment<A> extends Fragment<A> {
  constructor(public readonly text: string) {
    super();
  }
  public map<I>(): StringFragment<I> {
    return new StringFragment(this.text);
  }
  public prepare(): [string, []] {
    return [format.string(this.text), []];
  }
  public toString(): string {
    return this.text;
  }
}

class ArgumentFragment<A> extends Fragment<A> {
  constructor(public readonly accessor: Accessor<A>) {
    super();
  }

  public map<I>(mapper: Mapper<I, A>): ArgumentFragment<I> {
    return new ArgumentFragment((values) => this.accessor(mapper(values)));
  }

  public prepare(argOffset: number): [string, Accessor<A>[]] {
    return [`$${argOffset}`, [this.accessor]];
  }

  public toString(): string {
    return "<ARG>";
  }
}

class QueryFragment<A> extends Fragment<A> {
  constructor(public readonly fragments: Fragment<A>[]) {
    super();
  }

  public map<I>(mapper: Mapper<I, A>): QueryFragment<I> {
    return new QueryFragment(
      this.fragments.map((fragment) => fragment.map(mapper))
    );
  }

  public prepare(argOffset: number): [string, Accessor<A>[]] {
    return QueryFragment.staticPrepare(this.fragments, argOffset);
  }

  public static staticPrepare<A>(
    fragments: Fragment<A>[],
    argOffset: number
  ): [string, Accessor<A>[]] {
    const queryStrings: string[] = [];
    const accessors: Accessor<A>[] = [];
    fragments.forEach((fragment) => {
      const [fragmentString, fragmentAccessors] = fragment.prepare(
        argOffset + accessors.length
      );
      queryStrings.push(fragmentString);
      accessors.concat(fragmentAccessors);
    });
    return [queryStrings.join(""), accessors];
  }

  public toString(): string {
    return this.fragments.map((fragment) => fragment.toString()).join("");
  }
}

class LiteralFragment<A> extends Fragment<A> {
  constructor(public readonly value: Value) {
    super();
  }

  public map<I>(): LiteralFragment<I> {
    return new LiteralFragment(this.value);
  }

  public prepare(): [string, []] {
    return [format.literal(this.value), []];
  }

  public toString(): string {
    return `<VALUE ${JSON.stringify(this.value)}>`;
  }
}

class IdentifierFragment<A> extends Fragment<A> {
  constructor(public readonly name: string) {
    super();
  }
  public map<I>(): IdentifierFragment<I> {
    return new IdentifierFragment(this.name);
  }
  public prepare(): [string, []] {
    return [format.ident(this.name), []];
  }

  public toString(): string {
    return `<IDENT ${this.name}>`;
  }
}

export const sql = <A>(
  strings: TemplateStringsArray,
  ...args: (Value | Fragment<A> | BuildMethod<A>)[]
) => {
  const fragments: Fragment<A>[] = [
    new StringFragment(strings[0]),
    ...args.flatMap((arg, index) => {
      return [Fragment.from(arg), new StringFragment(strings[index + 1])];
    }),
  ];

  const build: BuildMethod<A> = Object.assign(
    <I>(mapper?: Mapper<I, A>) => {
      return mapper
        ? new QueryFragment(fragments.map((fragment) => fragment.map(mapper)))
        : new QueryFragment(fragments);
    },
    {
      query: <Row extends QueryResultRow = any>(
        conn: Pool | Client,
        values?: A
      ):
        | Promise<QueryResult<Row>>
        | ((values: A) => Promise<QueryResult<Row>>) => {
        const name: string = uuid.v4();
        const [query, accessors] = QueryFragment.staticPrepare(fragments, 0);

        const execute = async (values: A): Promise<QueryResult<Row>> => {
          const mappedValues = accessors.map((accessor) => accessor(values));

          const ret = await conn.query({
            name,
            text: query,
            values: mappedValues,
          });

          return ret;
        };

        if (values) {
          return execute(values);
        }

        return execute;
      },

      [isBuildMethodField]: true as true,
    }
  );

  return build;
};

interface ToString {
  toString(): string;
}

function baseTTL(
  strings: TemplateStringsArray | string,
  ...args: ToString[]
): string {
  if (typeof strings === "string") {
    return strings;
  }
  return [
    strings[0],
    ...args.flatMap((arg, index) => [arg.toString(), strings[index + 1]]),
  ].join("");
}

export function i<A>(
  strings: TemplateStringsArray,
  ...args: ToString[]
): IdentifierFragment<A> {
  return new IdentifierFragment<A>(baseTTL(strings, ...args));
}

export function l<A>(
  strings: TemplateStringsArray,
  ...args: ToString[]
): LiteralFragment<A> {
  return new LiteralFragment<A>(baseTTL(strings, ...args));
}

export function a<A extends { [key: string]: any }>(
  name: keyof A | TemplateStringsArray
): ArgumentFragment<A> {
  if (typeof name === "string") {
    return new ArgumentFragment((args) => args[name]);
  }
  if (typeof name === "object" && Array.isArray(name) && name.length === 1) {
    return new ArgumentFragment<A>((args) => args[name[0]]);
  }
  throw new TypeError("Argument builder received invalid arguments");
}

const limit = sql<{ limit: number }>`
    LIMIT ${a`limit`}
`;

const test = sql<{ name: string; limit: number }>`
    SELECT ${a`name`} FROM ${i`test`} ${limit};
`;
