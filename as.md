# 1. Functional Requirements (FR)

### 1.1. MSP Admin Console (Google Sheets)

* **FR-001 (Bulk Trigger):** The system must allow MSP Admins to select multiple suppliers from a list and trigger an outreach campaign via a custom menu item.
* **FR-002 (Status Tracking):** The system must update the status of each supplier in the master list (e.g., "Outreach Sent", "Data Received") in real-time.
* **FR-003 (Data Aggregation):** All employee data submitted by disparate suppliers must be normalized and aggregated into a single "Staged Data" sheet.

### 1.2. Supplier Portal (Workato Workflow App)

* **FR-004 (Frictionless Access):** Suppliers must access the portal via a secure, unique "Magic Link" sent to their email. No account creation or password management shall be required.
* **FR-005 (Dual Entry Method):** The portal must accept data via two methods:
1. **File Upload:** Parsing a standardized CSV template.
2. **Manual Entry:** A grid/form interface for smaller suppliers.


* **FR-006 (Instructional Context):** The landing page must provide clear context and a downloadable CSV template.

### 1.3. Orchestration (Workato Recipes)

* **FR-007 (Tokenization):** The system must generate unique workflow tokens for each supplier to ensure data isolation.
* **FR-008 (Payload Handling):** The system must parse multi-line JSON payloads from Google Apps Script.

---

# 2. Solution Design Specification (SDS)

### 2.1. System Architecture

The solution utilizes a **Hub-and-Spoke** architecture where Google Sheets acts as the database of record, and Workato acts as the logic engine and user interface.

1. **Trigger:** Google Apps Script collects row data -> POSTs JSON to Workato Webhook.
2. **Process (Dispatch):** Workato Recipe iterates through JSON -> Creates Workflow Task -> Emails Link.
3. **Interaction:** Supplier accesses Workflow App -> Submits Data.
4. **Process (Ingest):** Workato Recipe triggers on "Task Complete" -> Parses Data -> Writes to Google Sheet.

### 2.2. Data Schema (Google Sheets)

**Tab 1: `Supplier_Master**`
| Column | Key | Description |
| :--- | :--- | :--- |
| A | `Supplier_ID` | Unique UUID or internal ID. |
| B | `Supplier_Name` | Display name of the vendor. |
| C | `Contact_Email` | Destination for the Magic Link. |
| D | `Status` | Picklist: *Not Started, Queued, Outreach Sent, Completed*. |
| E | `Workflow_URL` | (Optional) For debugging, stores the generated link. |

**Tab 2: `Staged_Employees**`
| Column | Description |
| :--- | :--- |
| A | `Supplier_Ref_ID` (Foreign Key) |
| B..F | `First_Name`, `Last_Name`, `Job_Title`, `Start_Date`, `Rate` |

### 2.3. Technology Stack

* **Frontend:** Workato Workflow Apps (No-code UI).
* **Middleware:** Workato Recipes (2 distinct recipes).
* **Backend / DB:** Google Sheets.
* **Scripting:** Google Apps Script (for custom menu and payload construction).

---

# 3. Implementation Guide

### Phase 1: The Command Center (Google Sheets & GAS)

**Step 1.1: Setup Sheets**
Create a new Google Sheet with the two tabs defined in the SDS (`Supplier_Master`, `Staged_Employees`).

**Step 1.2: The Google Apps Script**
Open **Extensions > Apps Script**. Paste the following code. This script grabs the active row(s) and sends them to Workato.

*Note: You will update the `WORKATO_WEBHOOK_URL` after Phase 3.*

```javascript
function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('MSP Actions')
      .addItem('🚀 Initiate Outreach', 'triggerWorkatoOutreach')
      .addToUi();
}

function triggerWorkatoOutreach() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('Supplier_Master');
  const ui = SpreadsheetApp.getUi();
  
  // Get selected range or active rows
  const selection = sheet.getSelection();
  const ranges = selection.getActiveRangeList().getRanges();
  
  let payload = [];
  
  ranges.forEach(range => {
    const startRow = range.getRow();
    const numRows = range.getNumRows();
    const data = sheet.getRange(startRow, 1, numRows, 3).getValues(); // Assuming cols A, B, C
    
    data.forEach((row, index) => {
      // Basic validation to skip empty rows
      if(row[0] && row[2]) {
        payload.push({
          "supplier_id": row[0],
          "supplier_name": row[1],
          "email": row[2],
          "row_index": startRow + index
        });
        // Update Status to 'Queued' immediately for visual feedback
        sheet.getRange(startRow + index, 4).setValue("Queued");
      }
    });
  });

  if (payload.length === 0) {
    ui.alert('No valid rows selected.');
    return;
  }

  // Send to Workato
  const WORKATO_WEBHOOK_URL = 'YOUR_WEBHOOK_URL_HERE';
  
  const options = {
    'method' : 'post',
    'contentType': 'application/json',
    'payload' : JSON.stringify({ "suppliers": payload })
  };

  try {
    UrlFetchApp.fetch(WORKATO_WEBHOOK_URL, options);
    ui.alert(`Success! ${payload.length} suppliers queued for outreach.`);
  } catch (e) {
    ui.alert('Error connecting to Workato: ' + e.toString());
  }
}

```

### Phase 2: The Supplier Portal (Workato Workflow App)

**Step 2.1: Create the App**

1. Go to **Workflow Apps** in Workato. Create a new app: "MSP Supplier Portal."
2. **Page 1 (Start):**
* Add a **Rich Text** widget: "Welcome to the Client Onboarding Portal."
* Add a **File Download** widget: Link to a hosted "Employee_Template.csv".


3. **Page 2 (Submission):**
* Add a **File Upload** widget (Key: `upload_file`).
* *Optional:* Add a **Data Grid** for manual entry if you want to show off UI versatility.


4. **Page 3 (End):**
* Simple "Thank you" page.



### Phase 3: The Orchestration (Recipes)

**Recipe A: The Dispatcher (Webhook Listener)**

1. **Trigger:** HTTP Webhook (Accepts payload from GAS).
* *Copy this URL and paste it into your Google Apps Script.*


2. **Action:** **Repeat action** for each item in `list_of_suppliers`.
* **Call Workflow App:** "Create Task" (Select the App created in Phase 2).
* Pass `Supplier_Name` and `Supplier_ID` as task data.


* **Email:** Send email to `Supplier_Email` with the `Task_Link`.
* **Google Sheets:** Update Row (using `row_index` from payload) -> Set Status to "Outreach Sent."



**Recipe B: The Processor (Submission Handler)**

1. **Trigger:** Workflow App -> "New Task Completed."
2. **Action:** **IF** `upload_file` is present:
* **CSV Parser:** Parse the file content.
* **Bulk Add Rows:** Google Sheets -> `Staged_Employees`.
* Map CSV columns to Sheet columns.
* Hardcode `Supplier_ID` from the Workflow Task Data.




3. **Action:** Google Sheets -> Update Row (Supplier Master) -> Set Status to "Data Received."

### Phase 4: Demo Execution Checklist

1. Populate `Supplier_Master` with 3 dummy rows (use your own email aliases for testing).
2. Refresh the Sheet to load the "MSP Actions" menu.
3. Select the rows and click **"Initiate Outreach."**
4. Show the status change to "Queued" (GAS) then "Outreach Sent" (Recipe A).
5. Open your email, click the Magic Link, and upload a CSV.
6. Show the data appearing in `Staged_Employees`.
