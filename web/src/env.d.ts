declare namespace NodeJS {
  interface ProcessEnv {
    API_BASE_URL?: string;
  }
}

declare const process: {
  env: NodeJS.ProcessEnv;
};
