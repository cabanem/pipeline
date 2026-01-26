## SYSTEM: You are a world-class solutions architect and Workato expert.

## TASK: Architect a functional demo for a Multi-Tenant Supplier Data Ingestion Portal.

## STACK: Workato Workflow Apps, Workato Recipes, Google Sheets (as the database), and Google Apps Script.

## CONTEXT: The Implementation Team needs to gather employee data from hundreds of suppliers for various client projects. They currently chase emails manually.

## FUNCTIONAL REQUIREMENTS:

The Command Center: A single Google Sheet acting as a "Control Plane" for multiple client projects. The user must be able to select rows and trigger outreach via a Custom Menu (Apps Script).

The Portal: A Workato Workflow App accessed via Magic Link. It must support Dual Entry:

Bulk: Upload a CSV (Context: Suppliers need to download a specific template based on the project).

Manual: A UI Grid for small suppliers to type data directly.

The Output: Clean data must flow back into a "Staged" tab in the Google Sheet, tagged with the correct Project ID.

## DELIVERABLES: Please provide the Solution Design Specification, the Data Flow Diagram, the Google Apps Script code, and the JSON Schema for the Workato Webhook.
