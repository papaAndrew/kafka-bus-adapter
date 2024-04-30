import {
  BindingScope,
  Provider,
  ValueOrPromise,
  injectable,
} from "@loopback/core";
import { exit } from "process";
import { ErrorHandler } from "../lib/types";

@injectable({ scope: BindingScope.SINGLETON })
export class FatalErrorHandlerProvider implements Provider<ErrorHandler> {
  constructor() {}

  value(): ValueOrPromise<ErrorHandler> {
    return (err?: any) => {
      console.error({
        message: "Handle fatal Error",
        exception: JSON.stringify(err),
      });
      exit(1);
    };
  }
}
