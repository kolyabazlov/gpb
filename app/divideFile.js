import fs from "fs";
import csv from "fast-csv";
import chardet from 'chardet';
import iconv from 'iconv-lite';
import csvWriter from 'csv-write-stream';

// TODO: ����������� � �����������, ��� ��������� ��� �� ����
// TODO: ����������� ��� ������ ����� ������ � ������

const MAX_ROWS_IN_A_FILE = 20000; // ������������ ���������� ����� � �����
const CSV_PARSE_OPTIONS = { headers: true, delimiter: ';' };
const CSV_WRITER_OPTIONS = {separator: ';', newline: '\n', headers: undefined, sendHeaders: true};

async function divideFile (fileName, fullFilePath, writePath) {
    const fileEncode = await chardet.detectFile(fullFilePath); // ��������� ������ ����� ��� ����������� �������������

    return new Promise((resolve, reject) => {
        let dividedFilePaths = [];

        const reader = fs.createReadStream(fullFilePath)
            .pipe(iconv.decodeStream(fileEncode)); // ���� readable ������ � ������������� (���� �� ������� .csv ����� �� �������������� nodejs)
        const parser = csv.parseStream(reader, CSV_PARSE_OPTIONS); // Parser ������ ��� ����������� ������ ���������

        let writer = csvWriter(CSV_WRITER_OPTIONS); // Writable ����� ��� ����������� ������ ������ ���������
        let part = 1;
        writer.pipe(fs.createWriteStream(writePath + `${fileName}_part_${part}.csv`));

        let rowCounter = 0;
        parser
            .on('error', (err) => {
                dividedFilePaths.push(writePath + `${fileName}_part_${part}.csv`);

                console.log(`Error while dividing ${fileName}_part_${part}.csv: ${err}`)
                resolve(dividedFilePaths);
            })
            .on('data', (rowData) => {
                writer.write(rowData);
                rowCounter++;

                if (rowCounter >= MAX_ROWS_IN_A_FILE) { // ����� ������ ��� ������ ���������� ����� - ������ ����������� ������ ������
                    dividedFilePaths.push(writePath + `${fileName}_part_${part}.csv`);
                    part++;
                    rowCounter = 0;
                    writer.unpipe();
                    writer.pipe(fs.createWriteStream(writePath + `${fileName}_part_${part}.csv`));
                }
            })
            .on('end', () => {
                dividedFilePaths.push(writePath + `${fileName}_part_${part}.csv`);

                console.log(`Dividing ${fileName} has been ended.`)
                resolve(dividedFilePaths);
            })
    })
}

export { divideFile };