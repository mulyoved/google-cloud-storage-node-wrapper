"use strict";
const gcsstorage = require("@google-cloud/storage");
const GoogleCloudStorage = require("../index");
const chai = require("chai");
const assert = chai.assert;
const bufferEqual = require("buffer-equal");
const request = require("request");
const fs = require("fs");
const path = require("path");
const logger = console.log.bind(console);
const TEST_TIMEOUT = 30000;
const keyFile = require("../key.json");
const gcsConfig = {
    projectId: process.env["GCS_CONFIIG_PROJECT"],
    credentials: {
        client_email: process.env["GCS_CONFIIG_CLIENT_EMAIL"],
        private_key: process.env["GCS_CONFIIG_PRIVATE_KEY"]
    }
};
const gcsOptions = {
    loggingFunction: logger,
    bucket: "gcs-client-lib-testing"
};
const gcsWrapper = new GoogleCloudStorage(gcsConfig, gcsOptions);
describe("Google Cloud Storage Wrapper", () => {
    before(function (done) {
        // Clean bucket before tests.
        gcsstorage(gcsConfig)
            .bucket(gcsOptions.bucket)
            .deleteFiles()
            .then(() => {
            done();
        }).catch((error) => {
            console.log('Failed on cleaning bucket with reason, ', error);
        });
        this.timeout(TEST_TIMEOUT);
    });
    describe("UPLOADING method `save`", () => {
        it("should upload JSON object to the storage, read it back and compare", function (done) {
            let objectToSend = { str: "value", num: 85.543, bool: true, arr: [1, 2, 3], obj: { foo: "bar" } };
            let gcsPath = "test-folder-1:object.json";
            gcsWrapper.save(gcsPath, objectToSend).then(() => {
                gcsWrapper.readAsObject(gcsPath).then((rcvObject) => {
                    assert.deepEqual(objectToSend, rcvObject, "Sent and received objects are not deep-equal");
                    done();
                }).catch(done);
            }).catch(done);
        });
        it("should upload Buffer to the storage, read it back and compare", function (done) {
            let buffer = fs.readFileSync(path.resolve(__dirname, "./files/tesla.png"));
            let gcsPath = "test-folder-1:tesla.png";
            gcsWrapper.save(gcsPath, buffer).then(() => {
                gcsWrapper.readAsBuffer(gcsPath).then((rcvdBuffer) => {
                    assert.ok(bufferEqual(buffer, rcvdBuffer), "Sent and received buffers are not equal");
                    done();
                }).catch(done);
            }).catch(done);
        });
        it("should upload file from local filesystem to the storage, read it back and compare", function (done) {
            let fileName = path.resolve(__dirname, "./files/tesla.png");
            let buffer = fs.readFileSync(fileName);
            let gcsPath = "test-folder-1:tesla.png";
            gcsWrapper.save(gcsPath, fileName).then(() => {
                gcsWrapper.readAsBuffer(gcsPath).then((rcvdBuffer) => {
                    assert.ok(bufferEqual(buffer, rcvdBuffer), "Sent and received buffers are not equal");
                    done();
                }).catch(done);
            }).catch(done);
        });
        it("should upload file from local filesystem to the storage using stream, read it back and compare", function (done) {
            let fileName = path.resolve(__dirname, "./files/tesla.png");
            let buffer = fs.readFileSync(fileName);
            let stream = fs.createReadStream(fileName);
            let gcsPath = "test-folder-1:tesla.png";
            gcsWrapper.save(gcsPath, stream).then(() => {
                gcsWrapper.readAsBuffer(gcsPath).then((rcvdBuffer) => {
                    assert.ok(bufferEqual(buffer, rcvdBuffer), "Sent and received buffers are not equal");
                    done();
                }).catch(done);
            }).catch(done);
        });
        it("should upload file from local filesystem to the storage, read it back and compare (with compression)", function (done) {
            this.timeout(TEST_TIMEOUT);
            let fileName = path.resolve(__dirname, "./files/tesla.png");
            let buffer = fs.readFileSync(fileName);
            let gcsPath = "test-folder-1:tesla.png";
            gcsWrapper.save(gcsPath, fileName, { compress: true }).then(() => {
                gcsWrapper.readAsBuffer(gcsPath).then((rcvdBuffer) => {
                    assert.ok(bufferEqual(buffer, rcvdBuffer), "Sent and received buffers are not equal");
                    done();
                }).catch(done);
            }).catch(done);
        });
        it("should upload image with specified content type and then retrieve it via HTTP", function (done) {
            // For some reason sometimes fails, it may be some delay when file becomes publicly accessible.
            this.timeout(6000);
            let fileName = path.resolve(__dirname, "./files/tesla.png");
            let buffer = fs.readFileSync(fileName);
            let gcsPath = "test-folder-1:tesla.png";
            let contentType = "image/jpeg";
            gcsWrapper.save(gcsPath, fileName, { contentType: contentType, getURL: true }).then((url) => {
                assert.ok(typeof url === "string", "File URL should be a string");
                request.get(url, { encoding: null }, (err, res, body) => {
                    assert.equal(res.statusCode, 200, "Status code was not 200");
                    assert.equal(res.headers["content-type"], contentType, "Content type is wrong for retrieved blob");
                    assert.ok(bufferEqual(body, buffer), "Sent object is not equal with object retrieved by URL");
                    done();
                });
            }).catch(done);
        });
        it("should upload an object with additional metadata and then access the metadata via list()", function (done) {
            let fileName = "test-folder-1:metadata.json";
            let metadataValue = Math.random().toString();
            gcsWrapper.save(fileName, { a: 1 }, { metadata: { key: metadataValue } }).then(() => {
                gcsWrapper.list("test-folder-1:").then((list) => {
                    let file = list.find((item) => item.fullBlobName === fileName);
                    assert.ok(file.metadata["key"] === metadataValue, "Sent and received metadata value should be equal");
                    done();
                }).catch(done);
            }).catch(done);
        });
        it("should upload file from local filesystem to the storage using stream, read it back and write it localy", function (done) {
            let fileName = path.resolve(__dirname, "./files/tesla.png");
            let buffer = fs.readFileSync(fileName);
            let stream = fs.createReadStream(fileName);
            let gcsPath = "test-folder-1:tesla.png";
            let pathTOCopy = path.resolve(__dirname, "./files/tesla-copy.png");
            gcsWrapper.save(gcsPath, stream).then(() => {
                let writableStream = fs.createWriteStream(pathTOCopy);
                gcsWrapper.read(gcsPath, writableStream);
                writableStream.on("finish", () => {
                    let bufferOfDownloadedFile = fs.readFileSync(pathTOCopy);
                    assert.ok(bufferEqual(buffer, bufferOfDownloadedFile), "Sent and received buffers are not equal");
                    fs.unlinkSync(pathTOCopy);
                    done();
                });
            }).catch(done);
        });
    });
    describe("LISTING", function () {
        let gcsPathPrefix = "test-folder-1:";
        it("should return an array of files with specified prefix", function (done) {
            this.timeout(TEST_TIMEOUT);
            gcsWrapper.list(gcsPathPrefix).then((list) => {
                assert.ok(Array.isArray(list), "List should be an array");
                list.forEach((item) => assert.ok(item.fullBlobName.indexOf(gcsPathPrefix) !== -1, "Results contain item(s) from other folders"));
                done();
            }).catch(done);
        });
    });
    describe("REMOVING", function () {
        it("should upload file and then remove it", function (done) {
            this.timeout(TEST_TIMEOUT);
            let gcsPath = "test-folder-2:object.json";
            gcsWrapper.save(gcsPath, { a: 1 }).then(() => {
                gcsWrapper.delete(gcsPath).then((deleted) => {
                    assert.ok(deleted, "Blob wasn\"t deleted");
                    gcsWrapper.list("test-folder-2:").then(list => {
                        let file = list.filter((item) => item.fullBlobName === gcsPath);
                        assert.ok(file.length === 0, "Listing should not contain deleted blob");
                        done();
                    }).catch(done);
                }).catch(done);
            }).catch(done);
        });
    });
    describe("RETRY", () => {
        it("should call tryToDoOrFail and succeed only on 3 time", function (done) {
            this.timeout(TEST_TIMEOUT);
            let numberOfCall = 0;
            gcsWrapper.tryToDoOrFail(() => {
                return new Promise((resolve, reject) => {
                    if (numberOfCall < 2) {
                        numberOfCall++;
                        reject(new Error("No internet connection."));
                    }
                    else {
                        resolve(true);
                    }
                });
            }).then((result) => {
                done();
            });
        });
        it("should fail on tryToDoOrFail after 3 times trying to succeed", function (done) {
            this.timeout(TEST_TIMEOUT);
            let numberOfCall = 0;
            let errorMesssage = new Error("No internet connection.");
            gcsWrapper.tryToDoOrFail(() => {
                return new Promise((resolve, reject) => {
                    reject(errorMesssage);
                });
            }).catch((error) => {
                assert.ok(error, "Error is not empty");
                done();
            });
        });
    });
});
