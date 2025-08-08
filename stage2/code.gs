function showUploadDialog() {
  const html = HtmlService.createHtmlOutputFromFile('UploadForm')
    .setWidth(400)
    .setHeight(250);
  SpreadsheetApp.getUi().showModalDialog(html, 'Upload CSV File');
}
function showDateRangeForm() {
  const html = HtmlService.createHtmlOutputFromFile('DateRangeForm')
    .setWidth(400)
    .setHeight(300);
  SpreadsheetApp.getUi().showModalDialog(html, 'Select Date Range');
}

// ✅ تابع عمومی برای فیلتر کردن داده‌های تکراری
function filterUniqueRows(existingKeys, newRows, keyColumns) {
  const uniqueRows = [];

  newRows.forEach(row => {
    // ساخت کلید بر اساس ستون‌های مشخص شده
    const keyParts = keyColumns.map(idx => {
      const val = (row[idx] || '').toString().trim();
      return val.toUpperCase();
    });
    const key = keyParts.join('|');

    if (key && !existingKeys.has(key)) {
      existingKeys.add(key);
      uniqueRows.push(row);
    }
  });

  return uniqueRows;
}

function processCSVData(csvContent) {
  try {
    Logger.log("📥 Start processing actual CSV");

    if (!csvContent || typeof csvContent !== 'string') {
      throw new Error("CSV content is empty or not a string");
    }

    const cleanContent = csvContent.replace(/^﻿/, '').trim();
    const data = Utilities.parseCsv(cleanContent);
    if (!data || data.length < 2) throw new Error("CSV is empty or invalid");

    const csvHeaders = data[0];
    const rows = data.slice(1);
    const ss = SpreadsheetApp.getActiveSpreadsheet();

    const columnMap = {
      confirmed: 'Confirmed',
      txId: 'Transaction ID',
      transactionAddress: 'Wallet address',
      status: 'Status',
      type: 'Type',
      paymentGatewayName: 'Payment gateway',
      finalAmount: 'Transaction amount',
      finalCurrency: 'Transaction currency',
      transactionAmount: 'Settlement amount',
      transactionCurrencyDisplayName: 'Settlement currency',
      processingFee: 'Processing fee',
      price: 'Price',
      comment: 'Comment',
      paymentId: 'Payment ID',
      created: 'Booked',
      tradingAccount: 'Trading account',
      correctCoinSent: 'correctCoinSent',
      balanceAfterTransaction: 'Balance after',
      txId_2: 'Transaction ID',
      tierFee: 'Tier fee'
    };

    const sheetHeaders = Object.keys(columnMap);
    const sheetMap = {
      'M2p Deposit': [],
      'Settlement Deposit': [],
      'M2p Withdraw': [],
      'Settlement Withdraw': []
    };

    let addedRows = 0;

    // ✅ ساخت map داده‌ها
    rows.forEach((row) => {
      const rowObj = csvHeaders.reduce((obj, h, i) => {
        obj[h.trim()] = row[i];
        return obj;
      }, {});

      const txId = rowObj[columnMap.txId]?.trim();
      const status = (rowObj[columnMap.status] || '').toUpperCase();
      const pgName = (rowObj[columnMap.paymentGatewayName] || '').toUpperCase();
      const type = (rowObj[columnMap.type] || '').toUpperCase();

      if (!txId || pgName === 'BALANCE' || status !== 'DONE') return;

      const finalRow = sheetHeaders.map(key => {
        if (key === 'price') return '1';
        if (key === 'correctCoinSent') return 'TRUE';
        return rowObj[columnMap[key]] ?? '';
      });

      const targetSheet =
        type === 'DEPOSIT'
          ? (pgName.includes('SETTLEMENT') ? 'Settlement Deposit' : 'M2p Deposit')
          : (pgName.includes('SETTLEMENT') ? 'Settlement Withdraw' : 'M2p Withdraw');

      sheetMap[targetSheet].push(finalRow);
      addedRows++;
    });

    Logger.log("✅ Rows assigned: " + addedRows);

    let totalAdded = 0;

    // ✅ حلقه برای ذخیره داده‌ها
    Object.entries(sheetMap).forEach(([sheetName, newRows]) => {
      if (newRows.length === 0) return;

      let sheet = ss.getSheetByName(sheetName);
      if (!sheet) {
        sheet = ss.insertSheet(sheetName);
        sheet.appendRow(sheetHeaders);
      }

      const txIdIndex = sheetHeaders.indexOf('txId');
      const existingKeys = new Set();

      const existingRows = sheet.getLastRow();
      if (existingRows > 1) {
        const existingTxIds = sheet
          .getRange(2, txIdIndex + 1, existingRows - 1)
          .getValues()
          .flat()
          .map(id => id.toString().trim().toUpperCase());
        existingTxIds.forEach(id => existingKeys.add(id));
      }

      const uniqueRows = filterUniqueRows(existingKeys, newRows, [txIdIndex]);
      totalAdded += uniqueRows.length;

      if (uniqueRows.length > 0) {
        const startRow = sheet.getLastRow() + 1;
        sheet.getRange(startRow, 1, uniqueRows.length, sheetHeaders.length).setValues(uniqueRows);
      }

      Logger.log(`📄 Sheet: ${sheetName}, Added: ${uniqueRows.length}`);
    });

    // ✅ نمایش پیغام نهایی
    SpreadsheetApp.getUi().alert(`✅ Payment Data uploaded successfully!\nTotal Added Rows: ${totalAdded}`);
    Logger.log("✅ CSV import complete.");
  } catch (err) {
    Logger.log("❌ Error in processCSVData: " + err.message);
    SpreadsheetApp.getUi().alert("❌ Error in processCSVData: " + err.message);
    throw err;
  }
}


function showUploadDialog_IBRebate() {
  const html = HtmlService.createHtmlOutputFromFile('UploadIBRebate')
    .setWidth(400)
    .setHeight(250);
  SpreadsheetApp.getUi().showModalDialog(html, 'Upload IB Rebate File');
}

function uploadIBRebateCSV(csvContent) {
  try {
    const cleanContent = csvContent.replace(/^\uFEFF/, '').trim();
    const data = Utilities.parseCsv(cleanContent);
    if (!data || data.length < 2) throw new Error("CSV is empty or invalid");

    const headers = data[0];
    const rows = data.slice(1);

    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let sheet = ss.getSheetByName("IB Rebate");
    let isNewSheet = false;

    if (!sheet) {
      sheet = ss.insertSheet("IB Rebate");
      isNewSheet = true;
    }

    const idIndex = headers.findIndex(h => h.trim().toUpperCase() === 'TRANSACTION ID');
    const rebateTimeIndex = headers.findIndex(h => h.trim().toUpperCase() === 'REBATE TIME');
    if (idIndex === -1 || rebateTimeIndex === -1) throw new Error("Required columns not found");

    // ✅ گرفتن کلیدهای موجود
    const existingKeys = new Set();
    if (sheet.getLastRow() > 1) {
      const existingData = sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getLastColumn()).getValues();
      existingData.forEach(row => {
        const key = (row[idIndex] || '').toString().trim().toUpperCase();
        existingKeys.add(key);
      });
    }

    // ✅ فیلتر یکتا
    const uniqueRows = filterUniqueRows(existingKeys, rows, [idIndex]);

    if (uniqueRows.length > 0) {
      if (isNewSheet || sheet.getLastRow() === 0) {
        sheet.appendRow(headers);
      }
      sheet.getRange(sheet.getLastRow() + 1, 1, uniqueRows.length, headers.length)
           .setValues(uniqueRows);
    }

    // ✅ به‌روزرسانی تاریخ در A1
    const allRebateTimes = sheet.getRange(2, rebateTimeIndex + 1, sheet.getLastRow() - 1)
      .getValues()
      .flat()
      .map(t => new Date(t))
      .filter(d => d instanceof Date && !isNaN(d));

    if (allRebateTimes.length > 0) {
      const minDate = new Date(Math.min(...allRebateTimes));
      const maxDate = new Date(Math.max(...allRebateTimes));
      const text = `Report from ${minDate.toISOString().split('T')[0]} to ${maxDate.toISOString().split('T')[0]}`;
      sheet.getRange('A1').setValue(text);
    }
    SpreadsheetApp.getUi().alert(`✅ IB Rebate uploaded successfully!\nAdded rows: ${uniqueRows.length}`);


    Logger.log("✅ IB Rebate uploaded successfully.");
  } catch (err) {
    Logger.log("❌ Error in uploadIBRebateCSV: " + err.message);
    throw err;
  }
}

function showUploadDialog_CRMWithdrawals() {
  const html = HtmlService.createHtmlOutputFromFile('UploadCRMWithdrawals')
    .setWidth(400)
    .setHeight(250);
  SpreadsheetApp.getUi().showModalDialog(html, 'Upload CRM Withdrawals File');
}
function uploadCRMWithdrawalsCSV(csvContent) {
  try {
    const cleanContent = csvContent.replace(/^\uFEFF/, '').trim();
    const firstLine = cleanContent.split('\n')[0];
    const separator = detectSeparator(firstLine);
    const data = cleanContent.split('\n').map(line => line.split(separator));

    if (!data || data.length < 2) throw new Error("CSV is empty or invalid");

    // ✅ پاکسازی BOM، فاصله‌ها، کاراکترهای اضافی
    const headers = data[0].map(h => h.replace(/[\uFEFF\r\n]/g, '').trim());
    const headersUpper = headers.map(h => h.toUpperCase());
    const rows = data.slice(1);

    // ✅ تابع پیدا کردن ایندکس (انعطاف‌پذیر)
    function findIndexByNames(names) {
      return headersUpper.findIndex(h =>
        names.some(name => h === name.toUpperCase())
      );
    }

    // ✅ پیدا کردن ستون‌ها
    const reqTimeIdx = findIndexByNames(['Review Time']);
    const tradingAccountIdx = findIndexByNames(['Trading Account']);
    const amountIdx = findIndexByNames(['Withdrawal Amount']);
    const requestIdIdx = findIndexByNames(['Request ID']);

    // ✅ اگر ستون‌ها پیدا نشدند، نام‌های موجود را لاگ کنیم
    const missing = [];
    if (reqTimeIdx === -1) missing.push("Review Time");
    if (tradingAccountIdx === -1) missing.push("Trading Account");
    if (amountIdx === -1) missing.push("Withdrawal Amount");
    if (requestIdIdx === -1) missing.push("Request ID");

    if (missing.length > 0) {
      Logger.log("Headers found: " + JSON.stringify(headers));
      throw new Error(`❌ Required columns not found: ${missing.join(', ')}`);
    }

    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let sheet = ss.getSheetByName("CRM Withdrawals");
    if (!sheet) {
      sheet = ss.insertSheet("CRM Withdrawals");
      sheet.appendRow(headers);
    }

    // ✅ گرفتن Request ID های موجود
    const existingIDs = new Set();
    if (sheet.getLastRow() > 1) {
      const existingData = sheet.getRange(2, requestIdIdx + 1, sheet.getLastRow() - 1).getValues();
      existingData.forEach(id => {
        if (id[0]) existingIDs.add(id[0].toString().trim().toUpperCase());
      });
    }

    // ✅ تبدیل Withdrawal Amount
    rows.forEach(row => {
      const val = (row[amountIdx] || '').toString().trim().toUpperCase();
      if (val.includes('USD')) {
        row[amountIdx] = parseFloat(val.replace(/[^0-9.-]/g, ''));
      } else if (val.includes('USC')) {
        const rawAmount = parseFloat(val.replace(/[^0-9.-]/g, ''));
        row[amountIdx] = isNaN(rawAmount) ? '' : rawAmount / 100;
      } else {
        const rawAmount = parseFloat(val.replace(/[^0-9.-]/g, ''));
        row[amountIdx] = isNaN(rawAmount) ? '' : rawAmount;
      }
    });

    // ✅ فیلتر یکتا بر اساس Request ID
    const uniqueRows = rows.filter(row => {
      const id = row[requestIdIdx] ? row[requestIdIdx].toString().trim().toUpperCase() : '';
      if (id && !existingIDs.has(id)) {
        existingIDs.add(id);
        return true;
      }
      return false;
    });

    if (uniqueRows.length > 0) {
      sheet.getRange(sheet.getLastRow() + 1, 1, uniqueRows.length, headers.length).setValues(uniqueRows);
    }

    SpreadsheetApp.getUi().alert(`✅ CRM Withdrawals uploaded successfully!\nAdded rows: ${uniqueRows.length}`);
  } catch (err) {
    Logger.log("❌ Error in uploadCRMWithdrawalsCSV: " + err.message);
    SpreadsheetApp.getUi().alert("❌ Error: " + err.message);
    throw err;
  }
}








// تابع تشخیص جداکننده
function detectSeparator(line) {
  const tabCount = (line.match(/\t/g) || []).length;
  const commaCount = (line.match(/,/g) || []).length;
  const semicolonCount = (line.match(/;/g) || []).length;

  if (tabCount >= commaCount && tabCount >= semicolonCount) return '\t';
  if (semicolonCount >= commaCount) return ';';
  return ',';
}




function showUploadDialog_AccountList() {
  const html = HtmlService.createHtmlOutputFromFile('UploadAccountList')
    .setWidth(400)
    .setHeight(250);
  SpreadsheetApp.getUi().showModalDialog(html, 'Upload Account List File');
}

function uploadAccountListCSV(csvContent) {
  try {
    const cleanContent = csvContent.replace(/^\uFEFF/, '').trim();
    const allLines = cleanContent.split('\n');

    if (allLines[0].toUpperCase().includes("METATRADER")) {
      allLines.shift(); // حذف خط توضیحی اول
    }

    const data = Utilities.parseCsv(allLines.join('\n'), ';');
    if (!data || data.length < 2) throw new Error("CSV is empty or invalid");

    const headers = data[0].map(h => h.trim().toUpperCase());
    const rows = data.slice(1);

    const loginIndex = headers.indexOf("LOGIN");
    const nameIndex = headers.indexOf("NAME");
    const groupIndex = headers.indexOf("GROUP");

    if (loginIndex === -1 || nameIndex === -1 || groupIndex === -1)
      throw new Error("Required columns (Login, Name, Group) not found");

    const ss = SpreadsheetApp.getActiveSpreadsheet();

    // ✅ شیت Account List را پاک و دوباره بساز
    let sheet = ss.getSheetByName("Account List");
    if (sheet) ss.deleteSheet(sheet);
    sheet = ss.insertSheet("Account List");
    sheet.appendRow(["Login", "Name", "Group"]);

    // ✅ شیت Welcome Bonus Account list را پاک و دوباره بساز
    let welcomeSheet = ss.getSheetByName("Welcome Bonus Account list");
    if (welcomeSheet) ss.deleteSheet(welcomeSheet);
    welcomeSheet = ss.insertSheet("Welcome Bonus Account list");
    welcomeSheet.appendRow(["Login"]);

    const accountRows = [];
    const welcomeRows = [];

    rows.forEach(row => {
      const login = (row[loginIndex] || "").toString().trim();
      const name = (row[nameIndex] || "").toString().trim();
      const group = (row[groupIndex] || "").toString().trim();

      if (login) {
        accountRows.push([login, name, group]);
        if (group === "WELCOME\\Welcome BBOOK") {
          welcomeRows.push([login]);
        }
      }
    });

    if (accountRows.length > 0) {
      sheet.getRange(2, 1, accountRows.length, 3).setValues(accountRows);
    }

    if (welcomeRows.length > 0) {
      welcomeSheet.getRange(2, 1, welcomeRows.length, 1).setValues(welcomeRows);
    }
SpreadsheetApp.getUi().alert(`✅ Account List uploaded successfully!\nAccounts added: ${accountRows.length}\nWelcome Bonus accounts: ${welcomeRows.length}`);

    Logger.log("✅ Account List and Welcome Bonus Account list replaced successfully.");
  } catch (err) {
    Logger.log("❌ Error in uploadAccountListCSV: " + err.message);
    throw err;
  }
}

function showUploadDialog_CRMDeposit() {
  const html = HtmlService.createHtmlOutputFromFile('UploadCRMDeposit')
    .setWidth(400)
    .setHeight(250);
  SpreadsheetApp.getUi().showModalDialog(html, 'Upload CRM Deposit File');
}

function uploadCRMDepositCSV(csvContent) {
  try {
    const data = Utilities.parseCsv(csvContent.trim());
    if (!data || data.length < 2) throw new Error("CSV is empty or invalid");

    const headers = data[0].map(h => h.trim());
    const headersUpper = headers.map(h => h.toUpperCase());
    const rows = data.slice(1);

    const reqIdx = headersUpper.indexOf('REQUEST TIME');
    const accIdx = headersUpper.indexOf('TRADING ACCOUNT');
    const amtIdx = headersUpper.indexOf('TRADING AMOUNT');
    const idIdx = headersUpper.indexOf('REQUEST ID'); // برای حذف تکراری‌ها

    if ([reqIdx, accIdx, amtIdx].includes(-1))
      throw new Error("❌ Required columns not found (Request Time, Trading Account, Trading Amount)");

    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let sheet = ss.getSheetByName("CRM Deposit");
    const isNew = !sheet;

    if (isNew) {
      sheet = ss.insertSheet("CRM Deposit");
      sheet.appendRow(headers);
    }

    // ✅ گرفتن Request ID های موجود
    const existingIDs = new Set();
    if (sheet.getLastRow() > 1 && idIdx !== -1) {
      const existingData = sheet.getRange(2, idIdx + 1, sheet.getLastRow() - 1).getValues();
      existingData.forEach(id => {
        if (id[0]) existingIDs.add(id[0].toString().trim().toUpperCase());
      });
    }

    // ✅ فیلتر یکتا و تبدیل مقدار Trading Amount
    const uniqueRows = rows.filter(row => {
      if (idIdx !== -1 && row[idIdx]) {
        const id = row[idIdx].toString().trim().toUpperCase();
        if (!existingIDs.has(id)) {
          existingIDs.add(id);

          // ✅ پاکسازی و تبدیل مقدار ستون TRADING AMOUNT
          if (amtIdx !== -1 && row[amtIdx]) {
            const rawValue = row[amtIdx].toString().trim();
            const parts = rawValue.split(/\s+/); // جدا کردن واحد و عدد

            const unit = (parts[0] || '').toUpperCase();
            let numberPart = (parts[1] || '').replace(/,/g, '').replace(/[^\d.-]/g, '');
            let amount = parseFloat(numberPart) || 0;

            if (unit === 'USC') {
              amount = amount / 100; // تبدیل USC به USD
            }

            row[amtIdx] = amount; // درج عدد نهایی
          }

          return true;
        }
        return false;
      }
      return false;
    });

    if (uniqueRows.length > 0) {
      sheet.getRange(sheet.getLastRow() + 1, 1, uniqueRows.length, headers.length).setValues(uniqueRows);
    }

    SpreadsheetApp.getUi().alert(`✅ CRM Deposit uploaded successfully!\nAdded rows: ${uniqueRows.length}`);
  } catch (err) {
    Logger.log("❌ Error in uploadCRMDepositCSV: " + err.message);
    throw err;
  }
}









function generateFinalReport() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const reportSheetName = "Final Report";

  function sumColumn(sheet, columnName) {
    if (!sheet) return 0;
    const lastRow = sheet.getLastRow();
    if (lastRow < 2) return 0;

    const lastCol = sheet.getLastColumn();
    let headerRowIndex = null;
    let colIndex = -1;

    for (let row = 1; row <= 2 && row <= lastRow; row++) {
      const headerRow = sheet.getRange(row, 1, 1, lastCol).getValues()[0];
      const index = headerRow.findIndex(h =>
        h.toString().trim().toUpperCase() === columnName.trim().toUpperCase()
      );
      if (index !== -1) {
        colIndex = index;
        headerRowIndex = row;
        break;
      }
    }

    if (colIndex === -1) return 0;

    const numRows = lastRow - headerRowIndex;
    if (numRows <= 0) return 0;

    const data = sheet.getRange(headerRowIndex + 1, colIndex + 1, numRows).getValues().flat();
    return data.reduce((sum, val) => sum + (parseFloat(val) || 0), 0);
  }

  function findColumnIndex(headers, possibleNames) {
    return headers.findIndex(h => {
      const cleanHeader = h.toString().trim().toUpperCase();
      return possibleNames.some(name => cleanHeader === name.toUpperCase());
    });
  }

  let sheet = ss.getSheetByName(reportSheetName);
  if (!sheet) sheet = ss.insertSheet(reportSheetName);
  else sheet.clearContents();

  const values = [];

  // ✅ 1. جمع ریبیت
  const rebateSheet = ss.getSheetByName("IB Rebate");
  const rebateSum = rebateSheet ? sumColumn(rebateSheet, "Rebate") : 0;
  values.push(["Total Rebate", rebateSum]);

  // ✅ 2. دیپوزیت‌ها
  const clientDeposit = sumColumn(ss.getSheetByName("M2p Deposit"), "finalAmount");
  const settlementDeposit = sumColumn(ss.getSheetByName("Settlement Deposit"), "finalAmount");
  values.push(["M2p Deposit", clientDeposit]);
  values.push(["Settlement Deposit", settlementDeposit]);

  // ✅ 3. برداشت‌ها
  const clientWithdraw = sumColumn(ss.getSheetByName("M2p Withdraw"), "finalAmount");
  const settlementWithdraw = sumColumn(ss.getSheetByName("Settlement Withdraw"), "finalAmount");
  values.push(["M2p Withdrawal", clientWithdraw]);
  values.push(["Settlement Withdrawal", settlementWithdraw]);

  // ✅ 4. CRM Deposit
  const crmDeposit = sumColumn(ss.getSheetByName("CRM Deposit"), "Trading Amount");
  values.push(["CRM Deposit Total", crmDeposit]);

  // ✅ 5. Tier Fee Deposit
  const tierFeeDeposit1 = sumColumn(ss.getSheetByName("M2p Deposit"), "tierFee");
  const tierFeeDeposit2 = sumColumn(ss.getSheetByName("Settlement Deposit"), "tierFee");
  values.push(["Tier Fee Deposit", tierFeeDeposit1 + tierFeeDeposit2]);

  // ✅ 6. Tier Fee Withdraw
  const tierFeeWithdraw1 = sumColumn(ss.getSheetByName("M2p Withdraw"), "tierFee");
  const tierFeeWithdraw2 = sumColumn(ss.getSheetByName("Settlement Withdraw"), "tierFee");
  values.push(["Tier Fee Withdraw", tierFeeWithdraw1 + tierFeeWithdraw2]);

  // ✅ 7. Welcome Bonus Withdrawals
  // ✅ Welcome Bonus Withdrawals
let welcomeWithdrawSum = 0;
const welcomeSheet = ss.getSheetByName("Welcome Bonus Account list");
const crmWithdrawSheet = ss.getSheetByName("CRM Withdrawals");

let welcomeAccounts = [];
if (welcomeSheet && welcomeSheet.getLastRow() > 1) {
  welcomeAccounts = welcomeSheet.getRange(2, 1, welcomeSheet.getLastRow() - 1)
    .getValues()
    .flat()
    .map(v => v.toString().trim()); // همه را رشته کنیم
}
Logger.log("✅ Welcome Accounts: " + JSON.stringify(welcomeAccounts));

if (crmWithdrawSheet && welcomeAccounts.length > 0) {
  const headers = crmWithdrawSheet.getRange(2, 1, 1, crmWithdrawSheet.getLastColumn()).getValues()[0];
  const loginIdx = headers.findIndex(h => h.toString().trim().toUpperCase() === 'TRADING ACCOUNT');
  const amountIdx = headers.findIndex(h => h.toString().trim().toUpperCase() === 'WITHDRAWAL AMOUNT');

  Logger.log(`🟡 Indices — loginIdx: ${loginIdx}, amountIdx: ${amountIdx}`);

  if (loginIdx !== -1 && amountIdx !== -1) {
    const rows = crmWithdrawSheet.getRange(3, 1, crmWithdrawSheet.getLastRow() - 2, crmWithdrawSheet.getLastColumn()).getValues();
    Logger.log(`📄 Rows in CRM Withdrawals: ${rows.length}`);

    rows.forEach(row => {
      const loginCell = (row[loginIdx] || "").toString().trim();
      const loginMatch = loginCell.match(/\d+/); // گرفتن عدد لاگین
      const login = loginMatch ? loginMatch[0] : null;

      const amountRaw = row[amountIdx];
      const amount = parseFloat((amountRaw || "").toString().replace(/[^\d.-]/g, ''));

      Logger.log(`🔎 Checking → LoginCell: "${loginCell}", ParsedLogin: ${login}, AmountRaw: "${amountRaw}", ParsedAmount: ${amount}`);

      if (login && welcomeAccounts.includes(login)) {
        Logger.log(`✅ MATCH FOUND → ${login} in Welcome Accounts → Adding ${amount}`);
        if (!isNaN(amount)) {
          welcomeWithdrawSum += amount;
        }
      }
    });
  }
}
Logger.log(`💰 Welcome Bonus Withdraw Total: ${welcomeWithdrawSum}`);
values.push(["Welcome Bonus Withdrawals", welcomeWithdrawSum]);


  // ✅ 8. CRM TopChange Total
  let topchangeTotal = 0;
  const crmDepositSheet = ss.getSheetByName("CRM Deposit");
  if (crmDepositSheet) {
    const headers = crmDepositSheet.getRange(2, 1, 1, crmDepositSheet.getLastColumn()).getValues()[0];
    const payMethodIdx = findColumnIndex(headers, ["PAYMENT METHOD"]);
    const amountIdx = findColumnIndex(headers, ["TRADING AMOUNT"]);

    if (payMethodIdx !== -1 && amountIdx !== -1) {
      const data = crmDepositSheet.getRange(3, 1, crmDepositSheet.getLastRow() - 2, crmDepositSheet.getLastColumn()).getValues();
      data.forEach(row => {
        if ((row[payMethodIdx] || "").toString().toUpperCase() === "TOPCHANGE") {
          const amount = parseFloat((row[amountIdx] || "").toString().replace(/[^\d.]/g, ''));
          if (!isNaN(amount)) topchangeTotal += amount;
        }
      });
    }
  }
  values.push(["CRM TopChange Total", topchangeTotal]);

  // ✅ 9. CRM Withdraw Total
  const crmWithdraw = sumColumn(ss.getSheetByName("CRM Withdrawals"), "Withdrawal Amount");
  values.push(["CRM Withdraw Total", crmWithdraw]);

  // ✅ تاریخ‌های صحیح فقط از ستون‌های تاریخ
  const allDates = [];
  const sheetConfigs = [
    { name: "IB Rebate", dateCols: ["REBATE TIME"] },
    { name: "CRM Deposit", dateCols: ["REQUEST TIME"] },
    { name: "CRM Withdrawals", dateCols: ["Review Time"] },
    { name: "M2p Deposit", dateCols: ["CONFIRMED", "CREATED"] },
    { name: "M2p Withdraw", dateCols: ["CONFIRMED", "CREATED"] },
    { name: "Settlement Deposit", dateCols: ["CONFIRMED", "CREATED"] },
    { name: "Settlement Withdraw", dateCols: ["CONFIRMED", "CREATED"] }
  ];

  sheetConfigs.forEach(cfg => {
    const sh = ss.getSheetByName(cfg.name);
    if (!sh || sh.getLastRow() < 3) return;

    const headers = sh.getRange(2, 1, 1, sh.getLastColumn()).getValues()[0];
    cfg.dateCols.forEach(dateCol => {
      const idx = findColumnIndex(headers, [dateCol]);
      if (idx !== -1) {
        const data = sh.getRange(3, idx + 1, sh.getLastRow() - 2).getValues().flat();
        data.forEach(val => {
          const d = new Date(val);
          if (!isNaN(d.getTime())) allDates.push(d);
        });
      }
    });
  });

  const minDate = allDates.length > 0 ? new Date(Math.min(...allDates)) : new Date();
  const maxDate = allDates.length > 0 ? new Date(Math.max(...allDates)) : new Date();

  const startText = Utilities.formatDate(minDate, Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
  const endText = Utilities.formatDate(maxDate, Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
  sheet.getRange("A1").setValue(`From ${startText} to ${endText}`);


  // ✅ درج داده‌ها
  sheet.getRange(2, 1, values.length, 2).setValues(values);
}




function compareCRMandClientDeposits() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const crmSheet = ss.getSheetByName("CRM Deposit");
  const clientSheet = ss.getSheetByName("M2p Deposit");

  if (!crmSheet || !clientSheet) throw new Error("One or both sheets not found");

  const crmData = crmSheet.getDataRange().getValues();
  const clientData = clientSheet.getDataRange().getValues();
  if (crmData.length < 3 || clientData.length < 2) throw new Error("Sheets have no data");

  const crmHeaders = crmSheet.getRange(2, 1, 1, crmSheet.getLastColumn()).getValues()[0];
  const clientHeaders = clientSheet.getRange(1, 1, 1, clientSheet.getLastColumn()).getValues()[0];

  function findColIndex(headers, keyword) {
    return headers.findIndex(h => h.toString().replace(/\s+/g, '').toUpperCase() === keyword.replace(/\s+/g, '').toUpperCase());
  }

  const crmDateIdx = findColIndex(crmHeaders, "REQUEST TIME");
  const crmClientIdIdx = findColIndex(crmHeaders, "CLIENT ID");
  const crmAmountIdx = findColIndex(crmHeaders, "TRADING AMOUNT");
  const crmPayMethodIdx = findColIndex(crmHeaders, "PAYMENT METHOD");
  const crmNameIdx = findColIndex(crmHeaders, "NAME");

  const clientDateIdx = findColIndex(clientHeaders, "CONFIRMED");
  const clientAccountIdx = findColIndex(clientHeaders, "TRADINGACCOUNT");
  const clientAmountIdx = findColIndex(clientHeaders, "FINALAMOUNT");

  if ([crmDateIdx, crmClientIdIdx, crmAmountIdx, crmPayMethodIdx, crmNameIdx,
    clientDateIdx, clientAccountIdx, clientAmountIdx].includes(-1)) {
    throw new Error("Required columns not found");
  }

  const crmNormalized = crmData.slice(2).map((r, i) => ({
    rowIndex: i + 3,
    date: getRoundedDate(r[crmDateIdx]),
    clientId: (r[crmClientIdIdx] || "").toString().trim().toLowerCase(),
    name: (r[crmNameIdx] || "").toString().trim(),
    amount: parseFloat((r[crmAmountIdx] || "").toString().trim().replace(/[^\d.]/g, '')),
    payMethod: (r[crmPayMethodIdx] || "").toString().trim().toLowerCase(),
    source: "CRM Deposit"
  }));

  const clientNormalized = clientData.slice(1).map((r, i) => ({
    rowIndex: i + 2,
    date: getRoundedDate(r[clientDateIdx]),
    account: (r[clientAccountIdx] || "").toString().trim().toLowerCase(),
    amount: parseFloat((r[clientAmountIdx] || "").toString().trim().replace(/[^\d.]/g, '')),
    source: "M2p Deposit"
  }));

  const matched = new Set();
  const unmatched = [];

  crmNormalized.forEach(crmRow => {
    const match = clientNormalized.find(c =>
      Math.abs(c.date - crmRow.date) <= 3.5 * 3600 * 1000 &&
      c.account.includes(crmRow.clientId) &&
      Math.abs(c.amount - crmRow.amount) <= 1
    );

    if (!match && crmRow.payMethod !== 'topchange') {
      unmatched.push([crmRow.source, formatDate(crmRow.date), crmRow.clientId, "", crmRow.amount, crmRow.name, "", crmRow.rowIndex]);
    } else if (match) {
      matched.add(match);
    }
  });

  clientNormalized.forEach(cRow => {
    if ([...matched].includes(cRow)) return;

    const match = crmNormalized.find(crm =>
      Math.abs(cRow.date - crm.date) <= 3.5 * 3600 * 1000 &&
      cRow.account.includes(crm.clientId) &&
      Math.abs(crm.amount - cRow.amount) <= 1
    );

    if (!match) {
      unmatched.push([
        cRow.source,
        formatDate(cRow.date),
        "", // Client ID
        cRow.account,
        cRow.amount,
        "", // Client Name
        "", // Confirmed (Y/N)
        cRow.rowIndex
      ]);
    }
  });

  const headers = ["Source", "Date", "Client ID", "Trading Account", "Amount", "Client Name", "✅ Confirmed (Y/N)", "__RowIndex"];

  // ✅ ساخت شیت جدید با نام متفاوت
  let resultSheet = ss.getSheetByName("Deposit Discrepancies");
  if (resultSheet) ss.deleteSheet(resultSheet);  // اگر قبلاً بود، حذفش کن
  resultSheet = ss.insertSheet("Deposit Discrepancies");

  resultSheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  resultSheet.autoResizeColumns(1, headers.length);

  if (unmatched.length > 0) {
    unmatched.forEach(row => {
      while (row.length < headers.length) row.push("");
    });

    resultSheet.getRange(2, 1, unmatched.length, headers.length).setValues(unmatched);

    const confirmCol = 7;
    const validation = SpreadsheetApp.newDataValidation()
      .requireValueInList(["Y", "N"], true)
      .setAllowInvalid(false)
      .build();
    resultSheet.getRange(2, confirmCol, unmatched.length).setDataValidation(validation);
  } else {
    resultSheet.getRange(2, 1).setValue("✅ All rows matched");
  }
}

// توابع کمکی
function getRoundedDate(str) {
  const d = new Date(str);
  if (isNaN(d)) return 0;
  return d.getTime();
}

function formatDate(ms) {
  const d = new Date(ms);
  return Utilities.formatDate(d, Session.getScriptTimeZone(), "yyyy-MM-dd");
}
function removeConfirmedRowsFromDiscrepancySheet() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheetName = "Deposit Discrepancies"; // ← شیتی که مغایرت‌ها در آن ذخیره شده
  const crmSheet = ss.getSheetByName("CRM Deposit");
  const clientSheet = ss.getSheetByName("M2p Deposit");
  const sheet = ss.getSheetByName(sheetName);
  if (!sheet || !crmSheet || !clientSheet) {
    SpreadsheetApp.getUi().alert("❌ One or more required sheets not found.");
    return;
  }

  const data = sheet.getDataRange().getValues();
  const headers = data[0];
  const confirmIdx = headers.indexOf("✅ Confirmed (Y/N)");
  const sourceIdx = headers.indexOf("Source");
  const rowIndexIdx = headers.indexOf("__RowIndex");

  if ([confirmIdx, sourceIdx, rowIndexIdx].includes(-1)) {
    SpreadsheetApp.getUi().alert("❌ Required columns not found.");
    return;
  }

  const confirmedRows = data.slice(1)
    .map((row, i) => ({ row, sheetRow: i + 2 })) // sheetRow: actual row number in comparison sheet
    .filter(obj => (obj.row[confirmIdx] || "").toString().toUpperCase() === 'Y');

  // حذف معکوس برای اینکه ترتیب به‌هم نریزه
  confirmedRows.reverse().forEach(obj => {
    const source = obj.row[sourceIdx];
    const rowIndex = parseInt(obj.row[rowIndexIdx]);

    const targetSheet = source === "CRM Deposit" ? crmSheet : clientSheet;
    if (!isNaN(rowIndex)) {
      try {
        targetSheet.deleteRow(rowIndex);
        sheet.deleteRow(obj.sheetRow);
      } catch (err) {
        Logger.log("⚠️ Error deleting row " + rowIndex + ": " + err);
      }
    }
  });

  SpreadsheetApp.getUi().alert("✅ Confirmed rows deleted.");
}