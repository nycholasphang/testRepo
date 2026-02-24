using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Windows;
using System.Windows.Controls;
using Forms = System.Windows.Forms;

namespace testRepo;

public partial class MainWindow : Window
{
    private static readonly Dictionary<string, string> FieldByTransactionType = new(StringComparer.OrdinalIgnoreCase)
    {
        ["loyalty"] = "order_refno",
        ["walletReloadByEbanking"] = "clientReference",
        ["walletReloadByCard"] = "clientReference"
    };

    private static readonly Dictionary<string, string> EndpointHintByType = new(StringComparer.OrdinalIgnoreCase)
    {
        ["loyalty"] = "/api/aliments/pos/loyalty",
        ["walletReloadByEbanking"] = "/api/payment/walletReloadByEbanking",
        ["walletReloadByCard"] = "/api/payment/walletReloadByCard"
    };

    public MainWindow()
    {
        InitializeComponent();
        TransactionTypeComboBox.SelectionChanged += TransactionTypeComboBox_SelectionChanged;
        UpdateExpectedField();
    }

    private void TransactionTypeComboBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        UpdateExpectedField();
    }

    private void UpdateExpectedField()
    {
        var selectedType = GetSelectedType();
        ExpectedFieldTextBox.Text = FieldByTransactionType.GetValueOrDefault(selectedType, string.Empty);
    }

    private void BrowseFolder_Click(object sender, RoutedEventArgs e)
    {
        using var dialog = new Forms.FolderBrowserDialog
        {
            Description = "Select folder containing log files",
            UseDescriptionForTitle = true,
            ShowNewFolderButton = true
        };

        if (dialog.ShowDialog() == Forms.DialogResult.OK)
        {
            FolderPathTextBox.Text = dialog.SelectedPath;
        }
    }

    private async void RunButton_Click(object sender, RoutedEventArgs e)
    {
        try
        {
            OutputTextBox.Clear();

            var folderPath = FolderPathTextBox.Text.Trim();
            if (string.IsNullOrWhiteSpace(folderPath) || !Directory.Exists(folderPath))
            {
                WriteOutput("Please select a valid folder path.");
                return;
            }

            var selectedType = GetSelectedType();
            if (!FieldByTransactionType.TryGetValue(selectedType, out var targetField))
            {
                WriteOutput($"Unsupported transaction type: {selectedType}");
                return;
            }

            var connectionString = ConnectionStringTextBox.Text.Trim();
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                WriteOutput("Connection string is required.");
                return;
            }

            var endpointHint = EndpointHintByType.GetValueOrDefault(selectedType);
            var toPatchFolder = Path.Combine(folderPath, "toPatch");
            Directory.CreateDirectory(toPatchFolder);

            var files = Directory.GetFiles(folderPath, "*.log", SearchOption.TopDirectoryOnly);
            if (files.Length == 0)
            {
                WriteOutput("No .log files found in selected folder.");
                return;
            }

            WriteOutput($"Found {files.Length} log file(s). Type='{selectedType}', field='{targetField}'.");

            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            foreach (var filePath in files)
            {
                var fileName = Path.GetFileName(filePath);
                try
                {
                    var refNo = ExtractFieldValueFromLog(filePath, targetField, endpointHint);
                    if (string.IsNullOrWhiteSpace(refNo))
                    {
                        WriteOutput($"[{fileName}] Missing '{targetField}' in request payload. Skipped.");
                        continue;
                    }

                    var evaluation = await EvaluateTransactionAsync(connection, selectedType, refNo);
                    if (evaluation.IsComplete)
                    {
                        WriteOutput($"[{fileName}] {evaluation.Reason}. Ignored.");
                        continue;
                    }

                    var destination = EnsureUniquePath(Path.Combine(toPatchFolder, fileName));
                    File.Move(filePath, destination);
                    WriteOutput($"[{fileName}] {evaluation.Reason}. Moved to toPatch.");
                }
                catch (Exception ex)
                {
                    WriteOutput($"[{fileName}] Error: {ex.Message}");
                }
            }

            WriteOutput("Done.");
        }
        catch (Exception ex)
        {
            WriteOutput($"Fatal error: {ex.Message}");
        }
    }

    private static async Task<CheckEvaluation> EvaluateTransactionAsync(SqlConnection connection, string transactionType, string refNo)
    {
        var transactionId = await GetTransactionIdByReferenceAsync(connection, refNo);
        if (!transactionId.HasValue)
        {
            return CheckEvaluation.Incomplete($"RefNo '{refNo}' not found in transactions table");
        }

        if (transactionType.Equals("loyalty", StringComparison.OrdinalIgnoreCase))
        {
            var inLoyaltyPoints = await ExistsByTransactionIdAsync(connection, "loyaltypointstransactions", transactionId.Value);
            var inFuelTransactions = await ExistsByTransactionIdInAnyTableAsync(connection, transactionId.Value, "fueltransactions", "fueltransctions");

            if (inLoyaltyPoints || inFuelTransactions)
            {
                return CheckEvaluation.Complete($"RefNo '{refNo}' found (transactions.id={transactionId.Value}) and present in loyalty/fuel transaction table");
            }

            return CheckEvaluation.Incomplete($"RefNo '{refNo}' found in transactions.id={transactionId.Value} but missing in loyaltypointstransactions/fueltransactions");
        }

        var inCustomerCredit = await ExistsByTransactionIdAsync(connection, "customercredittransactions", transactionId.Value);
        if (inCustomerCredit)
        {
            return CheckEvaluation.Complete($"RefNo '{refNo}' found (transactions.id={transactionId.Value}) and present in customercredittransactions");
        }

        return CheckEvaluation.Incomplete($"RefNo '{refNo}' found in transactions.id={transactionId.Value} but missing in customercredittransactions");
    }

    private static async Task<long?> GetTransactionIdByReferenceAsync(SqlConnection connection, string refNo)
    {
        const string sql = "SELECT TOP (1) id FROM transactions WHERE refno = @RefNo ORDER BY id DESC";
        await using var command = new SqlCommand(sql, connection)
        {
            CommandType = CommandType.Text
        };
        command.Parameters.Add("@RefNo", SqlDbType.NVarChar, 128).Value = refNo;

        var result = await command.ExecuteScalarAsync();
        if (result is null || result == DBNull.Value)
        {
            return null;
        }

        return Convert.ToInt64(result);
    }

    private static async Task<bool> ExistsByTransactionIdAsync(SqlConnection connection, string tableName, long transactionId)
    {
        var sql = $"SELECT COUNT(1) FROM [{tableName}] WHERE transactionid = @TransactionId";
        await using var command = new SqlCommand(sql, connection)
        {
            CommandType = CommandType.Text
        };
        command.Parameters.Add("@TransactionId", SqlDbType.BigInt).Value = transactionId;

        var result = await command.ExecuteScalarAsync();
        if (result is null || result == DBNull.Value)
        {
            return false;
        }

        return Convert.ToInt32(result) > 0;
    }

    private static async Task<bool> ExistsByTransactionIdInAnyTableAsync(SqlConnection connection, long transactionId, params string[] tableNames)
    {
        foreach (var tableName in tableNames)
        {
            try
            {
                if (await ExistsByTransactionIdAsync(connection, tableName, transactionId))
                {
                    return true;
                }
            }
            catch (SqlException)
            {
                // ignore table-missing/invalid errors so we can try alternative spellings
            }
        }

        return false;
    }

    private string GetSelectedType()
    {
        return ((ComboBoxItem)TransactionTypeComboBox.SelectedItem).Content.ToString() ?? string.Empty;
    }

    private static string? ExtractFieldValueFromLog(string filePath, string fieldName, string? endpointHint)
    {
        var content = File.ReadAllText(filePath);
        if (!string.IsNullOrWhiteSpace(endpointHint) && !content.Contains(endpointHint, StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        foreach (var jsonCandidate in ExtractJsonObjects(content))
        {
            if (TryGetJsonProperty(jsonCandidate, fieldName, out var value) && !string.IsNullOrWhiteSpace(value))
            {
                return value;
            }
        }

        var fallbackPattern = $"\\b{Regex.Escape(fieldName)}\\b\\s*[:=]\\s*\"?([A-Za-z0-9\\-_.]+)\"?";
        var fallbackMatch = Regex.Match(content, fallbackPattern, RegexOptions.IgnoreCase);
        return fallbackMatch.Success ? fallbackMatch.Groups[1].Value.Trim() : null;
    }

    private static IEnumerable<string> ExtractJsonObjects(string content)
    {
        var objectRegex = new Regex("\\{(?:[^{}]|(?<open>\\{)|(?<-open>\\}))*(?(open)(?!))\\}", RegexOptions.Singleline);
        foreach (Match match in objectRegex.Matches(content))
        {
            yield return match.Value;
        }
    }

    private static bool TryGetJsonProperty(string json, string propertyName, out string? value)
    {
        value = null;
        try
        {
            using var doc = JsonDocument.Parse(json);
            if (doc.RootElement.ValueKind != JsonValueKind.Object)
            {
                return false;
            }

            foreach (var prop in doc.RootElement.EnumerateObject())
            {
                if (!string.Equals(prop.Name, propertyName, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                value = prop.Value.ValueKind switch
                {
                    JsonValueKind.String => prop.Value.GetString(),
                    JsonValueKind.Number => prop.Value.GetRawText(),
                    _ => prop.Value.GetRawText()
                };
                return true;
            }

            return false;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static string EnsureUniquePath(string path)
    {
        if (!File.Exists(path))
        {
            return path;
        }

        var directory = Path.GetDirectoryName(path)!;
        var fileNameWithoutExtension = Path.GetFileNameWithoutExtension(path);
        var extension = Path.GetExtension(path);
        var counter = 1;

        string candidate;
        do
        {
            candidate = Path.Combine(directory, $"{fileNameWithoutExtension}_{counter}{extension}");
            counter++;
        }
        while (File.Exists(candidate));

        return candidate;
    }

    private void WriteOutput(string message)
    {
        OutputTextBox.AppendText($"{DateTime.Now:HH:mm:ss} - {message}{Environment.NewLine}");
        OutputTextBox.ScrollToEnd();
    }

    private readonly record struct CheckEvaluation(bool IsComplete, string Reason)
    {
        public static CheckEvaluation Complete(string reason) => new(true, reason);
        public static CheckEvaluation Incomplete(string reason) => new(false, reason);
    }
}
