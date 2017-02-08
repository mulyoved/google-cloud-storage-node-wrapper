"use strict";

const gcsstorage = require("@google-cloud/storage");
const stream = require("stream");
const intoStream = require("into-stream");
const fs = require("fs");

interface IStorage {
    save(gcsPath: string, file: string): Promise<any>;
    read(gcsPath: string, writableStream: any): Promise<any>;
    readAsObject(gcsPath: string, options?: any): Promise<Object>;
    readAsBuffer(gcsPath: string, options?: any): Promise<Buffer>;
    list(prefix: string, options?: any): Promise<any>;
    delete(gcsPath: string): Promise<any>;
}

interface IGCSCredentials {
  client_email: string;
  private_key: string;
}

interface IgscConfig {
    projectId: string;
    keyFilename?: string;
    credentials?: IGCSCredentials;
}

interface Ioptions {
    loggingFunction?: any;
    bucket?: string;
    retriesCount?: number;
    retryInterval?: number;
}

interface IFile {
    fullBlobName: string;
    properties: any;
    metadata: any;
}

class GoogleCloudStorage implements IStorage {
    log: (...args) => void;
    gscConfig: any;
    storage: any;
    bucket: string;
    retriesCount: number;
    retryInterval: number;

    constructor(config: IgscConfig, options: Ioptions) {
        options = options || {};
        if (!config || !config.projectId || !config.keyFilename && !config.credentials) {
            throw new Error("Configuration object is invalid.");
        }
        if (config.credentials && config.credentials.private_key) {
          config.credentials.private_key = config.credentials.private_key.replace(/\\n/g, "\n");
        }
        this.bucket = options.bucket;
        this.storage = gcsstorage(config);
        this.log = options.loggingFunction || (() => null);
        this.retriesCount = options.retriesCount || 3;
        this.retryInterval = options.retryInterval || 500;
    }

    private getRemoteFileInstance (gcsPath: string): any {
      return this.storage.bucket(this.bucket).file(gcsPath);
    }

    async list(prefix: string, queryOptions?: any): Promise<IFile[]> {
        let query = {
            prefix: prefix || "",
            autoPaginate: false
        };
        // Expand query by additional parameters.
        // DOCS: https://googlecloudplatform.github.io/google-cloud-node/#/docs/storage/0.5.0/storage/bucket?method=getFiles
        if (queryOptions) {
          for (let option in queryOptions) {
            query[option] = queryOptions[option];
          }
        }
        let aStream = this.storage.bucket(this.bucket).getFilesStream(query);
        return this.readableStreamToPromise(aStream)
            .then((files) => {
                return files.map(this.transformGCSFileToFileInPrefferedFormat);
            });
    }

    private transformGCSFileToFileInPrefferedFormat (file: any): IFile {
      return {
        fullBlobName: file.name,
        properties: file.metadata || {},
        metadata: file.metadata.metadata || {}
      };
    }

    async delete(gcsPath: string): Promise<any> {
        let deletedFile = await this.getRemoteFileInstance(gcsPath).delete();
        return !!deletedFile;
    }

    private transformToStream(data: any) {
        if (data instanceof stream.Readable) {
            return data;
        } else if (Buffer.isBuffer(data)) {
            return this.convertBufferToStream(data);
        } else if (data instanceof Object) {
            return this.convertObjectIntoStream(data);
        } else if (typeof data === "string") {
            return fs.createReadStream(data);
        } else {
            throw new Error("Specified parameter has unsupported type.");
        }
    }

    async save(gcsPath: string, data: any, options?: any): Promise<any> {
        options = options || {};
        let aFile = this.getRemoteFileInstance(gcsPath);
        let gcsUploadOptions = {
            gzip: options.compress === true,
            public: true, // Make it optinal
            metadata: {
                contentType: options.contentType || "application/json",
                metadata: options.metadata || {}
            },
            resumable: false,
            validation: "crc32c"
        };
        let urlToFile = (options.getURL) ? this.buildUrlToFile(gcsPath) : null;
        let rStream = this.transformToStream(data);
        let gcsStream = aFile.createWriteStream(gcsUploadOptions);

        return this.tryToDoOrFail(() => {
            return this.uploadFile(rStream, gcsStream, urlToFile);
        });
    }

    private buildUrlToFile (gcsPath: string): string {
      return "https://" + this.bucket + ".storage.googleapis.com/" + gcsPath;
    }

    private async uploadFile(readableStream: any, writableStream: any, urlToFile: any) {
        return new Promise((resolve, reject) => {
            readableStream
                .pipe(writableStream)
                .on("error", function(err) {
                    reject(new Error(err));
                })
                .on("finish", function() {
                    resolve(urlToFile || true);
                });
        });
    }

    async read (gcsPath: string, writableStream: any): Promise<any> {
      return this.tryToDoOrFail(() => {
        return this.readAsBuffer(gcsPath).then((buffer) => {
          return new Promise ((resolve, reject) => {
            intoStream(buffer)
                    .pipe(writableStream)
                    .on("finish", resolve)
                    .on("error", reject);
            });
        });
      });
    }

    async readAsObject(gcsPath: string, options?: any): Promise<Object> {
      return this.tryToDoOrFail(() => {
        return this.readAsBuffer(gcsPath, options).then((buffer) => {
            let json = buffer.toString("utf8");
            return JSON.parse(json);
        });
      });
    }

    async readAsBuffer(gcsPath: string, options?: any): Promise<Buffer> {
      return this.tryToDoOrFail(() => {
        return this.getRemoteFileInstance(gcsPath)
                   .download()
                   .then((data) => data[0]);
      });
    }

    private async readableStreamToPromise(readableStream): Promise<any> {
        return new Promise((resolve, reject) => {
            let data = [];
            readableStream
                .on("error", reject)
                .on("data", (chunk) => {
                    data.push(chunk);
                })
                .on("end", () => {
                    resolve(data);
                });
        });
    }

    private convertObjectIntoStream(object: any): any {
        let stringifiedJson = JSON.stringify(object, null, 0);
        let buffer = new Buffer(stringifiedJson, "utf8");
        return this.convertBufferToStream(buffer);
    }

    private convertBufferToStream(buffer: Buffer) {
        return intoStream(buffer);
    }

    async tryToDoOrFail(asyncOperation) {
        for (let retriesCount = this.retriesCount; retriesCount > 0; retriesCount--) {
            try {
                this.log(`DBG:start:asyncOperation ${retriesCount}`);
                let rusultOfAsyncOperation = await asyncOperation();
                this.log(`DBG:end:asyncOperation ${retriesCount}`);
                return rusultOfAsyncOperation;
            } catch (error) {
                this.log(`Error while saving blob: ${error}. Retries left: ${retriesCount}`);
                await new Promise((resolve) => {
                    setTimeout(resolve, this.retryInterval);
                });
                // Last try failed.
                if (retriesCount === 1) {
                    throw new Error(error);
                }
            }
        }
    }
}

export = GoogleCloudStorage;
