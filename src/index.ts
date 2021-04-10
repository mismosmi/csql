import * as pg from "pg";
import format from "pg-format";
import * as uuid from "uuid";

// values that can be passed to postgres directly
type Value =
  | null
  | undefined
  | string
  | number
  | Date
  | Buffer
  | Value[]
  | { [key: string]: Value };

// access value from arguments
type Accessor<A> = (values: A) => Value;

// map between argument types when integrating one sql fragment in another
type Mapper<I, A> = (values: I) => A;

// return something that is already typed
type GenericQueryResult<Row> = {
  rows: Row[];
};

// interface for pg.Client or pg.Pool
interface Client {
  query(config: pg.QueryConfig): Promise<{ rows: pg.QueryResultRow[] }>;
}

// sql TTL returns a function with a property named 'query' of this type
// this function accepts a pg-client and returns a function that will actually
// execute the query on that client
interface QueryMethod<A> {
  <Row extends pg.QueryResultRow = any>(conn: Client, values?: A): (
    values?: A
  ) => Promise<GenericQueryResult<Row>>;
}

// the build-method is returned from the sql TTL
// it builds a QueryFragment that can be reused in further queries or
// turned into its own query by using its query-method
const isBuildMethodField = Symbol("isBuildMethod");
interface BuildMethod<A> {
  (): QueryFragment<A>;
  <I>(mapper: Mapper<I, A>): QueryFragment<I>;
  query: QueryMethod<A>;
  [isBuildMethodField]: true;
}

// type guard for the build method
function isBuildMethod<A>(fn: Function): fn is BuildMethod<A> {
  return isBuildMethodField in fn && fn[isBuildMethodField];
}

// the basic building block of a query
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

// a string is literal sql code
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

// an argument is something that can be passed to the query at call-time
// that is after calling `query`
// will show up in the final sql string as $1, $2, etc.
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

// a subquery consists of further fragments
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

// a literal will show up in the finished sql string passed to pg as
// a string encoded with single quotes or a raw number
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

// table names, column names, ...
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

// a value that is hard coded to the fragment built using the sql TTL
// but that will be passed as an argument to postgres.
// will show up in the final SQL string as $1, $2, ...
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

// options for the query-method
type QueryOptions<Row extends pg.QueryResultRow> = {
  validate?: (input: pg.QueryResultRow) => input is Row;
};

// the main component of this library
// build a fragment of an sql query using a TTL
export const sql = <A extends object>(
  strings: TemplateStringsArray,
  ...args: (Value | Fragment<A> | BuildMethod<A> | Accessor<A>)[]
) => {
  // build fragments from the arguments passed in using `${arg}` notation
  const fragments: Fragment<A>[] = [
    new StringFragment(strings[0]),
    ...args.flatMap((arg, index) => {
      return [Fragment.from(arg), new StringFragment(strings[index + 1])];
    }),
  ];

  // build the fragment
  // pass a mapper if needed
  // you need a mapper if you integrate one fragment in another and you need to
  // map between argument types
  const build: BuildMethod<A> = Object.assign(
    <I>(mapper?: Mapper<I, A>) => {
      return mapper
        ? new QueryFragment(fragments.map((fragment) => fragment.map(mapper)))
        : new QueryFragment(fragments);
    },
    {
      // turn a fragment into a callable query by calling this
      // pass in a pg.Client or pg.Pool
      query: <Row extends pg.QueryResultRow = any>(
        conn: Client,
        options?: QueryOptions<Row>
      ): ((values?: A) => Promise<GenericQueryResult<Row>>) => {
        // use a unique identifier for the query to enable caching
        // if the query is called multiple times on a single postgres connection
        const name: string = uuid.v4();
        const [query, accessors] = QueryFragment.staticPrepare(fragments, 1);

        // actually execute the query
        const execute = async (
          // TODO: make typescript check that this can't be called without
          // an argument if A is not {}
          values: A = {} as any
        ): Promise<GenericQueryResult<Row>> => {
          // build the array of arguments matching the $1, $2, ... placeholders
          const mappedValues = accessors.map((accessor) => accessor(values));

          // finally execute the query
          const ret = await conn.query({
            name,
            text: query,
            values: mappedValues,
          });

          // if type guard for the return type is given, use it
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

          // else assert type and return
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

// pass an identifier to the sql TTL as `i\`identifier\`` or `i('identifier')`
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

// pass a literal to the sql TTL as `l\`value\`` or `l(value)`
export function l<A>(
  strings: TemplateStringsArray | Value,
  ...args: ToString[]
): LiteralFragment<A> {
  if (isTemplateStringsArray(strings)) {
    return new LiteralFragment<A>(baseTTL(strings, ...args));
  }
  return new LiteralFragment<A>(strings);
}

// pass an argument by just giving the argument name as `a\`name\`` or `a('name')`
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
