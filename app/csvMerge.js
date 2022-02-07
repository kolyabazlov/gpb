import fs from "fs";
import csv from "fast-csv";
import csvWriter from "csv-write-stream";

const CSV_PARSE_OPTIONS = { headers: true, delimiter: ';' };
const CSV_WRITER_OPTIONS = {separator: ';', newline: '\n', headers: undefined, sendHeaders: true};

async function csvMerge (pathsArrays, resultFullPath) {
    let writer = csvWriter(CSV_WRITER_OPTIONS);
    let result = 0;

    let i = 0;
    for (const path of pathsArrays[0]) {
        //console.log('WORKING ON', pathsArrays[0][i], pathsArrays[1][i]);
        result += await compareToMemory(writer, await fileToMemory(pathsArrays[0][i]), pathsArrays[1][i], resultFullPath);
        i++;
    }

    return result;
}

function fileToMemory (filePath) {
    return new Promise((resolve, reject) => {
        let rowsArray = [];
        const readable = fs.createReadStream(filePath)
        const parser = csv.parseStream(readable, CSV_PARSE_OPTIONS);

        let counter = 0;
        parser
            .on('error', (err) => {
                console.log('Error in filToMemory:', err);
            })
            .on('data', (row) => {
                const [number, operator] = Object.values(row);
                rowsArray[counter] = { number, operator }
                counter++;
            })
            .on('end', () => {
                resolve(rowsArray);
            })
    })
}

function compareToMemory (writer, memoryArray, filePath, resultFullPath) {
    return new Promise((resolve, reject) => {
        const readable = fs.createReadStream(filePath)
        const parser = csv.parseStream(readable, CSV_PARSE_OPTIONS);

        writer.pipe(fs.createWriteStream(resultFullPath));

        let counter = 0;
        let totalLines = 0;
        parser
            .on('error', (err) => {
                console.log('Error in compareToMemory:', err);
            })
            .on('data', (row) => {
                const [number, operator] = Object.values(row);
                if (operator === memoryArray[counter]?.operator) {
                    totalLines++;
                    switch (operator) {
                        case '*':
                            writer.write({
                                first: memoryArray[counter]?.number,
                                operator: '*',
                                second: number,
                                total: memoryArray[counter]?.number * number,
                            })
                            break;
                        case '+':
                            writer.write({
                                first: memoryArray[counter]?.number,
                                operator: '+',
                                second: number,
                                total: memoryArray[counter]?.number + number,
                            })
                            break;
                        case '/':
                            writer.write({
                                first: memoryArray[counter]?.number,
                                operator: '/',
                                second: number,
                                total: memoryArray[counter]?.number / number,
                            })
                            break;
                        case '-':
                            writer.write({
                                first: memoryArray[counter]?.number,
                                operator: '-',
                                second: number,
                                total: memoryArray[counter]?.number - number,
                            })
                            break;
                    }
                }
                counter++;
            })
            .on('end', () => {
                resolve(totalLines);
            })
    })
}

export { csvMerge };