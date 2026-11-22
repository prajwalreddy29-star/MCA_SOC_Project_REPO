# Databricks notebook source
table_view_config = {
  "TableViewConfig": {
    "displayLength": 50000,
    "Resources": {
      "Term": {
        "Signifier": {
          "name": "FullName"
        },
        "DisplayName": {
          "name": "DisplayName"
        },
        "Id": {
          "name": "Id"
        },
        "Vocabulary": {
          "Name": {
            "name": "Domain_Name"
          },
          "Id": {
            "name": "Domain_Id"
          }
        },
        "ConceptType": {
          "Id": {
            "name": "AssetType_Id"
          },
          "Color": {
            "name": "AssetType_Color"
          },
          "SymbolType": {
            "name": "AssetType_SymbolType"
          },
          "Icon": {
            "name": "AssetType_IconCode"
          },
          "Acronym": {
            "name": "AssetType_AcronymCode"
          },
          "DisplayNameEnabled": {
            "name": "AssetType_DisplayNameEnabled"
          }
        },
        "StringAttribute": [
          {
            "labelId": "5cfccbc5-12b4-4f8b-b28e-a5936d5eb2bd",
            "LongExpression": {
              "name": "StringAttribute_5cfccbc5-12b4-4f8b-b28e-a5936d5eb2bd_Value"
            },
            "Id": {
              "name": "StringAttribute_5cfccbc5-12b4-4f8b-b28e-a5936d5eb2bd_Id"
            }
          },
          {
            "labelId": "eba078ee-aa4e-4b4a-8da4-7084fa874d12",
            "LongExpression": {
              "name": "StringAttribute_eba078ee-aa4e-4b4a-8da4-7084fa874d12_Value"
            },
            "Id": {
              "name": "StringAttribute_eba078ee-aa4e-4b4a-8da4-7084fa874d12_Id"
            }
          }
        ],
        "SingleValueListAttribute": [
          {
            "labelId": "915cb955-d296-4c7b-a3f3-18324e8d8922",
            "Value": {
              "name": "SingleValueListAttribute_915cb955-d296-4c7b-a3f3-18324e8d8922_Value"
            },
            "Id": {
              "name": "SingleValueListAttribute_915cb955-d296-4c7b-a3f3-18324e8d8922_Id"
            }
          }
        ],
        "Filter": {
          "OR": [
            {
              "Field": {
                "name": "Domain_Id",
                "operator": "EQUALS",
                "value": "c42bbd6d-71be-4c19-9d5b-8a038f590e00"
              }
            }
          ]
        },
        "Order": [
          {
            "Field": {
              "name": "Domain_Name",
              "order": "ASC"
            }
          }
        ]
      }
    },
    "Columns": [
      {
        "Column": {
          "fieldName": "DisplayName",
          "label": "Name"
        }
      },
      {
        "Column": {
          "fieldName": "Domain_Name",
          "label": "Domain"
        }
      },
      {
        "Column": {
          "fieldName": "StringAttribute_5cfccbc5-12b4-4f8b-b28e-a5936d5eb2bd_Value",
          "label": "Customer_Id"
        }
      },
      {
        "Column": {
          "fieldName": "StringAttribute_eba078ee-aa4e-4b4a-8da4-7084fa874d12_Value",
          "label": "Customer_Name"
        }
      },
      {
        "Column": {
          "fieldName": "SingleValueListAttribute_915cb955-d296-4c7b-a3f3-18324e8d8922_Value",
          "label": "Type"
        }
      }
    ]
  }
}
