export interface TestRequestOptions {
  readonly method?: string;
  readonly headers?: HeadersInit;
  readonly body?: BodyInit | null;
}

export function makeTestRequest(
  url = "https://example.test/resource",
  options: TestRequestOptions = {},
): Request {
  return new Request(url, {
    method: options.method ?? "GET",
    ...(options.headers === undefined ? {} : { headers: options.headers }),
    ...(options.body === undefined ? {} : { body: options.body }),
  });
}

export function makeTestResponse(
  status = 200,
  options: { readonly headers?: HeadersInit; readonly body?: BodyInit | null } = {},
): Response {
  return new Response(options.body, {
    status,
    ...(options.headers === undefined ? {} : { headers: options.headers }),
  });
}
