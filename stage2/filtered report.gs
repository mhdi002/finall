var globalStart = '';
var globalEnd = '';

function processDateFilteredReports(startStr, endStr) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const start = parseDate(startStr);
  const end = parseDate(endStr);

  globalStart = startStr;
  globalEnd = endStr;

  const filteredSuffix = ' (Filtered)';
  const targets = [
    { name: 'M2p Deposit', dateCol: 'created' },
    { name: 'Settlement Deposit', dateCol: 'created' },
    { name: 'M2p Withdraw', dateCol: 'created' },
    { name: 'Settlement Withdraw', dateCol: 'created' },
    { name: 'IB Rebate', dateCol: 'Rebate Time' },
    { name: 'CRM Withdrawals', dateCol: 'Review Time' },
    { name: 'CRM Deposit', dateCol: 'Request Time' }
  ];

  // ✅ حذف شیت‌های قبلی فیلتر شده
  ss.getSheets()
    .filter(s => s.getName().endsWith(filteredSuffix))
    .forEach(s => ss.deleteSheet(s));

  targets.forEach(({ name, dateCol }) => {
    const original = ss.getSheetByName(name);
    const newSheetName = name + filteredSuffix;

    if (ss.getSheetByName(newSheetName)) ss.deleteSheet(ss.getSheetByName(newSheetName));
    const sheet = ss.insertSheet(newSheetName);

    // ✅ ردیف اول → بازه تاریخ
    sheet.getRange(1, 1).setValue(`Filtered from ${startStr} to ${endStr}`);

    if (!original || original.getLastRow() < 2) {
      sheet.getRange(2, 1).setValue('No Data');
      return;
    }

    const data = original.getDataRange().getValues();
    if (data.length < 2) {
      sheet.getRange(2, 1).setValue('No Data');
      return;
    }

    // ✅ پیدا کردن هدر واقعی
    let headerRowIndex = -1;
    for (let i = 0; i < Math.min(3, data.length); i++) {
      if (data[i].some(cell => cell && cell.toString().toLowerCase().includes(dateCol.toLowerCase()))) {
        headerRowIndex = i;
        break;
      }
    }
    if (headerRowIndex === -1) headerRowIndex = 0; // پیش‌فرض

    const headers = data[headerRowIndex];
    sheet.getRange(2, 1, 1, headers.length).setValues([headers]); // ✅ هدر در ردیف 2

    const dateIdx = headers.findIndex(h => h.toString().trim().toLowerCase() === dateCol.toLowerCase());
    if (dateIdx === -1) return;

    const filteredRows = [];
    for (let i = headerRowIndex + 1; i < data.length; i++) {
      const row = data[i];
      const cell = row[dateIdx];
      const date = new Date(cell);
      if (!isNaN(date) && date >= start && date <= end) {
        filteredRows.push(row);
      }
    }

    // ✅ درج داده از ردیف 3 به بعد
    if (filteredRows.length > 0) {
      sheet.getRange(3, 1, filteredRows.length, headers.length).setValues(filteredRows);
    }
  });

  if (typeof generateFilteredFinalReport === 'function') {
    generateFilteredFinalReport();
  }
}

// ✅ پارس کردن تاریخ با فرمت dd.MM.yyyy HH:mm:ss
function parseDate(str) {
  if (!str) return null;
  const parts = str.split(' ');
  const datePart = parts[0];
  const timePart = parts[1] || '00:00:00';

  let day, month, year;
  if (datePart.includes('.')) {
    [day, month, year] = datePart.split('.').map(Number);
  } else if (datePart.includes('/')) {
    [month, day, year] = datePart.split('/').map(Number);
  } else {
    return null;
  }

  const [hour, minute, second] = timePart.split(':').map(x => Number(x) || 0);
  return new Date(year, month - 1, day, hour, minute, second);
}
