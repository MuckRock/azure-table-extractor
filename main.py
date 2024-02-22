"""
Extract tables from DocumentCloud documents with Azure Document Intelligence
"""
import os
import csv
import sys
import json
from documentcloud.addon import AddOn
from documentcloud.exceptions import APIError
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential


class TableExtractor(AddOn):
    """Extract tables using Azure DI"""
    def calculate_cost(self, documents):
        """ Given a set of documents, counts the number of pages and returns a cost"""
        total_num_pages = 0
        for doc in documents:
            start_page = self.data.get("start_page", 1)
            end_page = self.data.get("end_page")
            last_page = 0
            if end_page <= doc.page_count:
                last_page = end_page
            else:
                last_page = doc.page_count
            pages_to_analyze = last_page - start_page + 1
            total_num_pages += pages_to_analyze
        cost = total_num_pages * 7
        print(cost)
        return cost


    def validate(self):
        """Validate that we can run the analysis"""

        if self.get_document_count() is None:
            self.set_message(
                "It looks like no documents were selected. Search for some or "
                "select them and run again."
            )
            sys.exit(0)
        if not self.org_id:
            self.set_message("No organization to charge.")
            sys.exit(0)
        ai_credit_cost = self.calculate_cost(
            self.get_documents()
        )
        try:
            self.charge_credits(ai_credit_cost)
        except ValueError:
            return False
        except APIError:
            return False
        return True

    def get_table_data(self, result):
        """Extract table data from the result of the poller"""
        table_data = []

        for table in result.tables:
            table_info = {
                "page_number": table.page_number,
                "cells": []
            }

            # Extract cells from the current table
            for cell in table.cells:
                cell_info = {
                    "row_index": cell.row_index,
                    "column_index": cell.column_index,
                    "content": cell.text
                }
                table_info["cells"].append(cell_info)

            # Append table info to the list
            table_data.append(table_info)

        return table_data

    def convert_to_csv(self, table_data):
        """Convert table data to CSV format"""
        csv_data = []
        for table_info in table_data:
            page_number = table_info["page_number"]
            for cell in table_info["cells"]:
                row_index = cell["row_index"]
                column_index = cell["column_index"]
                content = cell["content"]
                csv_row = [page_number, row_index, column_index, content]
                csv_data.append(csv_row)
        return csv_data

    def main(self):
        """Validate, run the extraction on each document, save results in a zip file"""
        output_format = self.data.get("output_format", "json")
        start_page = self.data.get("start_page", 1)
        end_page = self.data.get("end_page", 1)

        if not self.validate():
            self.set_message(
                "You do not have sufficient AI credits to run this Add-On on this document set"
            )
            sys.exit(0)

        if end_page < start_page:
            self.set_message("The end page you provided is smaller than the start page, try again")
            sys.exit(0)
        if start_page < 1:
            self.set_message("Your start page is less than 1, please try again")
            sys.exit(0)

        # grab endpoint and API key for access from secrets
        key = os.environ.get("KEY")
        endpoint = os.environ.get("TOKEN")

        # authenticate
        document_analysis_client = DocumentAnalysisClient(
            endpoint=endpoint, credential=AzureKeyCredential(key)
        )

        for document in self.get_documents():
            table_data = []
            outer_bound = end_page + 1
            if end_page > document.page_count:
                outer_bound = document.page_count + 1
            for page_number in range(start_page, outer_bound):
                image_url = document.get_large_image_url(page_number)
                poller = document_analysis_client.begin_analyze_document_from_url(
                    "prebuilt-layout", image_url
                )
                result = poller.result()
                table_data.extend(self.get_table_data(result))

            if output_format == "json":
                table_data_json = json.dumps(table_data, indent=4)
                output_file_path = f"tables-{document.id}.json"
                with open(output_file_path, "w", encoding="utf-8") as json_file:
                    json_file.write(table_data_json)
            if output_format == "csv":
                output_file_path = f"tables-{document.id}.csv"
                with open(output_file_path, "a", newline="", encoding="utf-8") as csv_file:
                    writer = csv.writer(csv_file)
                    if csv_file.tell() == 0:
                        writer.writerow(["Page Number", "Row Index", "Column Index", "Content"])
                    csv_data = self.convert_to_csv(table_data)
                    writer.writerows(csv_data)


if __name__ == "__main__":
    TableExtractor().main()
