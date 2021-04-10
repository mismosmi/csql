import * as pg from "pg";
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

type GenericQueryResult<Row> = {
  rows: Row[];
};

interface Client {
  query(config: pg.QueryConfig): Promise<{ rows: pg.QueryResultRow[] }>;
}

interface QueryMethod<A> {
  <Row extends pg.QueryResultRow = any>(conn: Client, values?: A): (
    values?: A
  ) => Promise<GenericQueryResult<Row>>;
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

    return new ValueFragment(source);
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
    return `<ARG ${this.accessor.toString()}>`;
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
      accessors.push(...fragmentAccessors);
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
    return `<LITERAL ${JSON.stringify(this.value)}>`;
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

class ValueFragment<A> extends Fragment<A> {
  constructor(public readonly value: Value) {
    super();
  }

  public map<I>(): ValueFragment<I> {
    return new ValueFragment(this.value);
  }

  public prepare(argOffset: number): [string, Accessor<A>[]] {
    return [`$${argOffset}`, [() => this.value]];
  }

  public toString(): string {
    return `<VALUE ${JSON.stringify(this.value)}>`;
  }
}

type QueryOptions<Row extends pg.QueryResultRow> = {
  validate?: (input: pg.QueryResultRow) => input is Row;
};

export const sql = <A extends object>(
  strings: TemplateStringsArray,
  ...args: (Value | Fragment<A> | BuildMethod<A> | Accessor<A>)[]
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
      query: <Row extends pg.QueryResultRow = any>(
        conn: Client,
        options?: QueryOptions<Row>
      ): ((values?: A) => Promise<GenericQueryResult<Row>>) => {
        const name: string = uuid.v4();
        const [query, accessors] = QueryFragment.staticPrepare(fragments, 1);

        const execute = async (
          // TODO: make typescript check that this can't be called without
          // an argument if A is not {}
          values: A = {} as any
        ): Promise<GenericQueryResult<Row>> => {
          const mappedValues = accessors.map((accessor) => accessor(values));

          const ret = await conn.query({
            name,
            text: query,
            values: mappedValues,
          });

          if (options?.validate) {
            return {
              rows: ret.rows.map((row, index) => {
                if (!options.validate(row)) {
                  throw new TypeError(`Row ${index} is invalid`);
                }
                return row;
              }),
            };
          }

          return ret as GenericQueryResult<Row>;
        };

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

function baseTTL(strings: TemplateStringsArray, ...args: ToString[]): string {
  return [
    strings[0],
    ...args.flatMap((arg, index) => [arg.toString(), strings[index + 1]]),
  ].join("");
}

export function i<A>(
  strings: TemplateStringsArray | string,
  ...args: ToString[]
): IdentifierFragment<A> {
  if (typeof strings === "string") {
    return new IdentifierFragment<A>(strings);
  }
  return new IdentifierFragment<A>(baseTTL(strings, ...args));
}

function isTemplateStringsArray(
  obj: TemplateStringsArray | Value
): obj is TemplateStringsArray {
  return Array.isArray(obj) && "raw" in obj;
}

export function l<A>(
  strings: TemplateStringsArray | Value,
  ...args: ToString[]
): LiteralFragment<A> {
  if (isTemplateStringsArray(strings)) {
    return new LiteralFragment<A>(baseTTL(strings, ...args));
  }
  return new LiteralFragment<A>(strings);
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
