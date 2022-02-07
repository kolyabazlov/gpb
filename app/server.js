import express from 'express';
import formidable from 'formidable';
import path from 'path';
import {fileURLToPath} from 'url';
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
import fs from 'fs';
import { divideFile } from "./divideFile.js";
import {csvMerge} from "./csvMerge.js";

const app = express();
const PORT = process.env.PORT || 3000;
const TEMPORARY_FOLDER = path.join(__dirname, "../temp/");

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'))
});

// TODO: fix сервер останавливается при ошибке

app.post('/upload/', (req, res, next) => {
    const form = new formidable.IncomingForm({
        uploadDir: TEMPORARY_FOLDER,
        keepExtensions: true,
        multiples: true,
    });

    form
        .parse(req, (err, fields, files) => {
            const { filetoupload } = files;

            if (!filetoupload.length) {
                res.end('You did not select files');
            }

            if (filetoupload.length > 2) {
                res.end('You can merge only two files');
            }
            let resultFileNames = [];

            let arrayOfDivideFilePromises = filetoupload.map(({ filepath, newFilename, originalFilename }) => {

                resultFileNames.push(newFilename.split('.')[0]);

                if (originalFilename.split('.').pop() !== 'csv') {
                    return res.end('One of a files is not .csv extension');
                }

                return divideFile(newFilename.split('.')[0], filepath, TEMPORARY_FOLDER)
                    .then((result) => {
                        fs.unlink(filepath, () => console.log(`Loaded '${originalFilename}' was divided then deleted.`))
                        return result;
                    })
            })

            Promise.all(arrayOfDivideFilePromises)
                .then(pathsArrays => {
                    csvMerge(pathsArrays, TEMPORARY_FOLDER + resultFileNames.join('_') + '_result.csv')
                        .then((totalLines) => {
                            console.log('Total lines:', totalLines);
                            res.download(TEMPORARY_FOLDER + resultFileNames.join('_') + '_result.csv', 'result.csv');
                        })
                        .then(() => {
                            pathsArrays.push(TEMPORARY_FOLDER + resultFileNames.join('_') + '_result.csv');
                            pathsArrays.flat().map(filePath => fs.unlink(filePath, () => console.log(`Temporary file ${filePath} was deleted.`)))
                        })
                });
        })

    form
        .on('error', (err) => {
            res.send('Error happened:', err)
        })
});

app.listen(PORT, () => {
    console.log(`App ready on http://localhost:${PORT}/ `)
});
