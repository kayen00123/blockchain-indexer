const MAX_ENDPOINTS = 6;

export type WebsocketEndpointInput = string | string[];

export function normalizeWebsocketEndpoints(input: WebsocketEndpointInput, legacyUrl = ''): string[] {
  const values = Array.isArray(input) ? input : [input, legacyUrl];
  return [...new Set(values.flatMap((value) => String(value ?? '').split(/[\s,]+/).map((entry) => entry.trim()).filter(Boolean)))].slice(0, MAX_ENDPOINTS);
}

export function readWebsocketEndpoints(prefix: string, legacyUrl = ''): string[] {
  const values = [
    process.env[`${prefix}_WS_URLS`],
    ...Array.from({ length: MAX_ENDPOINTS }, (_, index) => process.env[`${prefix}_WS_URL_${index + 1}`]),
    legacyUrl,
  ];
  return [...new Set(values.flatMap((value) => String(value ?? '').split(/[\s,]+/).map((entry) => entry.trim()).filter(Boolean)))].slice(0, MAX_ENDPOINTS);
}

export function nextWebsocketEndpoint(endpoints: string[], currentIndex: number): { url: string; index: number } {
  if (endpoints.length === 0) return { url: '', index: 0 };
  const index = (currentIndex + 1) % endpoints.length;
  return { url: endpoints[index], index };
}
