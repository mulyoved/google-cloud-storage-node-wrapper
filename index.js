"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator.throw(value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments)).next());
    });
};
const gcsstorage = require("@google-cloud/storage");
const stream = require("stream");
const intoStream = require("into-stream");
const fs = require("fs");
class GoogleCloudStorage {
    constructor(config, options) {
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
    getRemoteFileInstance(gcsPath) {
        return this.storage.bucket(this.bucket).file(gcsPath);
    }
    list(prefix, queryOptions) {
        return __awaiter(this, void 0, void 0, function* () {
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
        });
    }
    transformGCSFileToFileInPrefferedFormat(file) {
        return {
            fullBlobName: file.name,
            properties: file.metadata || {},
            metadata: file.metadata.metadata || {}
        };
    }
    delete(gcsPath) {
        return __awaiter(this, void 0, void 0, function* () {
            let deletedFile = yield this.getRemoteFileInstance(gcsPath).delete();
            return !!deletedFile;
        });
    }
    transformToStream(data) {
        if (data instanceof stream.Readable) {
            return data;
        }
        else if (Buffer.isBuffer(data)) {
            return this.convertBufferToStream(data);
        }
        else if (data instanceof Object) {
            return this.convertObjectIntoStream(data);
        }
        else if (typeof data === "string") {
            return fs.createReadStream(data);
        }
        else {
            throw new Error("Specified parameter has unsupported type.");
        }
    }
    save(gcsPath, data, options) {
        return __awaiter(this, void 0, void 0, function* () {
            options = options || {};
            let aFile = this.getRemoteFileInstance(gcsPath);
            let gcsUploadOptions = {
                gzip: options.compress === true,
                public: true,
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
        });
    }
    buildUrlToFile(gcsPath) {
        return "https://" + this.bucket + ".storage.googleapis.com/" + gcsPath;
    }
    uploadFile(readableStream, writableStream, urlToFile) {
        return __awaiter(this, void 0, void 0, function* () {
            return new Promise((resolve, reject) => {
                readableStream
                    .pipe(writableStream)
                    .on("error", function (err) {
                    reject(new Error(err));
                })
                    .on("finish", function () {
                    resolve(urlToFile || true);
                });
            });
        });
    }
    read(gcsPath, writableStream) {
        return __awaiter(this, void 0, void 0, function* () {
            return this.tryToDoOrFail(() => {
                return this.readAsBuffer(gcsPath).then((buffer) => {
                    return new Promise((resolve, reject) => {
                        intoStream(buffer)
                            .pipe(writableStream)
                            .on("finish", resolve)
                            .on("error", reject);
                    });
                });
            });
        });
    }
    readAsObject(gcsPath, options) {
        return __awaiter(this, void 0, void 0, function* () {
            return this.tryToDoOrFail(() => {
                return this.readAsBuffer(gcsPath, options).then((buffer) => {
                    let json = buffer.toString("utf8");
                    return JSON.parse(json);
                });
            });
        });
    }
    readAsBuffer(gcsPath, options) {
        return __awaiter(this, void 0, void 0, function* () {
            return this.tryToDoOrFail(() => {
                return this.getRemoteFileInstance(gcsPath)
                    .download()
                    .then((data) => data[0]);
            });
        });
    }
    readableStreamToPromise(readableStream) {
        return __awaiter(this, void 0, void 0, function* () {
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
        });
    }
    convertObjectIntoStream(object) {
        let stringifiedJson = JSON.stringify(object, null, 0);
        let buffer = new Buffer(stringifiedJson, "utf8");
        return this.convertBufferToStream(buffer);
    }
    convertBufferToStream(buffer) {
        return intoStream(buffer);
    }
    tryToDoOrFail(asyncOperation) {
        return __awaiter(this, void 0, void 0, function* () {
            for (let retriesCount = this.retriesCount; retriesCount > 0; retriesCount--) {
                try {
                    this.log(`DBG:start:asyncOperation ${retriesCount}`);
                    let rusultOfAsyncOperation = yield asyncOperation();
                    this.log(`DBG:end:asyncOperation ${retriesCount}`);
                    return rusultOfAsyncOperation;
                }
                catch (error) {
                    this.log(`Error while saving blob: ${error}. Retries left: ${retriesCount}`);
                    yield new Promise((resolve) => {
                        setTimeout(resolve, this.retryInterval);
                    });
                    // Last try failed.
                    if (retriesCount === 1) {
                        throw new Error(error);
                    }
                }
            }
        });
    }
}
module.exports = GoogleCloudStorage;
