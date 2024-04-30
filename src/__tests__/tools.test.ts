import { omitUndefined } from "../lib/tools";

describe("Tool functions", () => {
  it("omitUndefined", () => {
    const ref = {
      a: 8,
    };
    const obj = {
      a: 8,
      b: undefined,
    };
    const res = omitUndefined(obj);

    console.log("res", res);

    expect(res).toMatchObject(ref);
  });
});
