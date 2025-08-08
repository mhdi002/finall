function generateFilteredFinalReport() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheetName = 'Final Report (Filtered)';
  let sheet = ss.getSheetByName(sheetName);
  if (sheet) ss.deleteSheet(sheet);
  sheet = ss.insertSheet(sheetName);

  // ✅ ردیف اول → بازه تاریخ
  sheet.getRange(1, 1).setValue(`Filtered from ${globalStart} to ${globalEnd}`);

  const values = [
    ['Total Rebate', sumFiltered('IB Rebate (Filtered)', 'Rebate')],
    ['M2p Deposit', sumFiltered('M2p Deposit (Filtered)', 'finalAmount')],
    ['Settlement Deposit', sumFiltered('Settlement Deposit (Filtered)', 'finalAmount')],
    ['M2p Withdrawal', sumFiltered('M2p Withdraw (Filtered)', 'finalAmount')],
    ['Settlement Withdrawal', sumFiltered('Settlement Withdraw (Filtered)', 'finalAmount')],
    ['CRM Deposit Total', sumFiltered('CRM Deposit (Filtered)', 'Trading Amount')],
    ['Topchange Deposit Total', calcTopchangeFiltered()],
    ['Tier Fee Deposit',
      sumFiltered('M2p Deposit (Filtered)', 'tierFee') +
      sumFiltered('Settlement Deposit (Filtered)', 'tierFee')
    ],
    ['Tier Fee Withdraw',
      sumFiltered('M2p Withdraw (Filtered)', 'tierFee') +
      sumFiltered('Settlement Withdraw (Filtered)', 'tierFee')
    ],
    ['Welcome Bonus Withdrawals', calcWelcomeBonusFiltered()],
    ['CRM Withdraw Total', sumFiltered('CRM Withdrawals (Filtered)', 'Withdrawal Amount')],
  ];

  // ✅ داده‌ها از ردیف 2 به بعد (هدر حذف شد)
  sheet.getRange(2, 1, values.length, 2).setValues(values);
}

// ✅ تابع کمکی برای جمع یک ستون در شیت‌های فیلترشده
function sumFiltered(sheetName, columnName) {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(sheetName);
  if (!sheet || sheet.getLastRow() < 3) return 0;

  const headers = sheet.getRange(2, 1, 1, sheet.getLastColumn()).getValues()[0];
  const idx = headers.findIndex(h => h.toString().trim().toUpperCase() === columnName.trim().toUpperCase());
  if (idx === -1) return 0;

  const data = sheet.getRange(3, idx + 1, sheet.getLastRow() - 2).getValues().flat();
  return data.reduce((sum, v) => sum + (parseFloat(v) || 0), 0);
}

// ✅ محاسبه Topchange فقط برای CRM Deposit (Filtered)
function calcTopchangeFiltered() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('CRM Deposit (Filtered)');
  if (!sheet || sheet.getLastRow() < 3) return 0;

  const headers = sheet.getRange(2, 1, 1, sheet.getLastColumn()).getValues()[0];
  const payMethodIdx = headers.findIndex(h => h.toString().trim().toUpperCase() === 'PAYMENT METHOD');
  const amountIdx = headers.findIndex(h => h.toString().trim().toUpperCase() === 'TRADING AMOUNT');

  if (payMethodIdx === -1 || amountIdx === -1) return 0;

  const data = sheet.getRange(3, 1, sheet.getLastRow() - 2, sheet.getLastColumn()).getValues();
  return data.reduce((sum, row) => {
    if ((row[payMethodIdx] || '').toString().toUpperCase() === 'TOPCHANGE') {
      const amount = parseFloat((row[amountIdx] || '').toString().replace(/[^\d.]/g, ''));
      if (!isNaN(amount)) sum += amount;
    }
    return sum;
  }, 0);
}

// ✅ محاسبه Welcome Bonus Withdrawals
function calcWelcomeBonusFiltered() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const welcomeSheet = ss.getSheetByName('Welcome Bonus Account list');
  const withdrawSheet = ss.getSheetByName('CRM Withdrawals (Filtered)');
  if (!welcomeSheet || !withdrawSheet || withdrawSheet.getLastRow() < 3) return 0;

  const welcomeAccounts = welcomeSheet.getRange(2, 1, welcomeSheet.getLastRow() - 1).getValues().flat().map(v => v.toString().trim());

  const headers = withdrawSheet.getRange(2, 1, 1, withdrawSheet.getLastColumn()).getValues()[0];
  const loginIdx = headers.findIndex(h => h.toString().trim().toUpperCase() === 'TRADING ACCOUNT');
  const amountIdx = headers.findIndex(h => h.toString().trim().toUpperCase() === 'WITHDRAWAL AMOUNT');

  if (loginIdx === -1 || amountIdx === -1) return 0;

  const rows = withdrawSheet.getRange(3, 1, withdrawSheet.getLastRow() - 2, withdrawSheet.getLastColumn()).getValues();
  return rows.reduce((sum, row) => {
    const login = (row[loginIdx] || '').toString().trim();
    const amount = parseFloat((row[amountIdx] || '').toString().replace(/[^\d.-]/g, ''));
    if (welcomeAccounts.includes(login) && !isNaN(amount)) {
      sum += amount;
    }
    return sum;
  }, 0);
}
