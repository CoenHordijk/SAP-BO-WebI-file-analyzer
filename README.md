Coen Hordijk 202404531

Here is some Python code to analyse SAP Business Objects WebI Report (.wid) files.

Discovering which variables in a report are used and which are not used is very complicated, especially in bulky reports.

This code takes a WID file as input and generates an Excelsheet showing all variable dependencies.

You might need to install some Python libraries to get the code running.

To process the WID file:

Open the notebook 'exportReportStatusToExcel.ipynb'.
Enter the right pathname and wid file name in the first cells.
Run the notebook.
An excelfile is created in the same directory as the WID-file, with the same name plus extension .xlsx
Tested with Business Objects 4.3 \ Jupyter Lab \ Python 3.12.1

Use this code at your own risk.

