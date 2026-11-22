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
          },
          "Signifier": {
            "name": "AssetType_Name"
          },
          "Id": {
            "name": "AssetType_Id"
          },
          "PublicId": {
            "name": "AssetType_PublicId"
          },
          "FinalType": {
            "name": "AssetType_FinalType"
          },
          "LockStatuses": {
            "name": "AssetType_LockStatuses"
          },
          "Product": {
            "name": "AssetType_Product"
          }
        },
        "Relation": [
          {
            "Order": [
              {
                "Field": {
                  "name": "TargetRelation_00000000-0000-0000-0000-000000007047_Source_FullName",
                  "order": "ASC"
                }
              }
            ],
            "typeId": "00000000-0000-0000-0000-000000007047",
            "type": "TARGET",
            "Id": {
              "name": "TargetRelation_00000000-0000-0000-0000-000000007047_Id"
            },
            "Source": {
              "DisplayName": {
                "name": "TargetRelation_00000000-0000-0000-0000-000000007047_Source_DisplayName"
              },
              "Signifier": {
                "name": "TargetRelation_00000000-0000-0000-0000-000000007047_Source_FullName"
              },
              "Id": {
                "name": "TargetRelation_00000000-0000-0000-0000-000000007047_Source_Id"
              }
            }
          },
          {
            "typeId": "00000000-0000-0000-0000-000000007094",
            "type": "SOURCE",
            "Id": {
              "name": "SourceRelation_00000000-0000-0000-0000-000000007094_Id"
            },
            "Target": {
              "DisplayName": {
                "name": "SourceRelation_00000000-0000-0000-0000-000000007094_Target_DisplayName"
              },
              "Signifier": {
                "name": "SourceRelation_00000000-0000-0000-0000-000000007094_Target_FullName"
              },
              "Id": {
                "name": "SourceRelation_00000000-0000-0000-0000-000000007094_Target_Id"
              }
            }
          },
          {
            "typeId": "585b1994-891b-44ca-8ed9-60dace9f1498",
            "type": "TARGET",
            "Id": {
              "name": "TargetRelation_585b1994-891b-44ca-8ed9-60dace9f1498_Id"
            },
            "Source": {
              "DisplayName": {
                "name": "TargetRelation_585b1994-891b-44ca-8ed9-60dace9f1498_Source_DisplayName"
              },
              "Signifier": {
                "name": "TargetRelation_585b1994-891b-44ca-8ed9-60dace9f1498_Source_FullName"
              },
              "Id": {
                "name": "TargetRelation_585b1994-891b-44ca-8ed9-60dace9f1498_Source_Id"
              }
            }
          },
          {
            "typeId": "ee120737-acc3-49d4-a55f-1ef9c55df1af",
            "type": "TARGET",
            "Id": {
              "name": "TargetRelation_ee120737-acc3-49d4-a55f-1ef9c55df1af_Id"
            },
            "Source": {
              "DisplayName": {
                "name": "TargetRelation_ee120737-acc3-49d4-a55f-1ef9c55df1af_Source_DisplayName"
              },
              "Signifier": {
                "name": "TargetRelation_ee120737-acc3-49d4-a55f-1ef9c55df1af_Source_FullName"
              },
              "Id": {
                "name": "TargetRelation_ee120737-acc3-49d4-a55f-1ef9c55df1af_Source_Id"
              }
            }
          }
        ],
        "StringAttribute": [
          {
            "labelId": "cfbb6bf5-00f1-4bc4-ac66-8bb9fa44245e",
            "LongExpression": {
              "name": "StringAttribute_cfbb6bf5-00f1-4bc4-ac66-8bb9fa44245e_Value"
            },
            "Id": {
              "name": "StringAttribute_cfbb6bf5-00f1-4bc4-ac66-8bb9fa44245e_Id"
            }
          },
          {
            "labelId": "15b9b3d4-c5e5-4d94-9a06-d3f45fccaa9e",
            "LongExpression": {
              "name": "StringAttribute_15b9b3d4-c5e5-4d94-9a06-d3f45fccaa9e_Value"
            },
            "Id": {
              "name": "StringAttribute_15b9b3d4-c5e5-4d94-9a06-d3f45fccaa9e_Id"
            }
          },
          {
            "labelId": "cd0543bd-6f32-443d-a05c-5f1971af6534",
            "LongExpression": {
              "name": "StringAttribute_cd0543bd-6f32-443d-a05c-5f1971af6534_Value"
            },
            "Id": {
              "name": "StringAttribute_cd0543bd-6f32-443d-a05c-5f1971af6534_Id"
            }
          },
          {
            "labelId": "14ae4838-7924-4237-aa59-cb0a6e1f245f",
            "LongExpression": {
              "name": "StringAttribute_14ae4838-7924-4237-aa59-cb0a6e1f245f_Value"
            },
            "Id": {
              "name": "StringAttribute_14ae4838-7924-4237-aa59-cb0a6e1f245f_Id"
            }
          },
          {
            "labelId": "fcba2ffb-414f-485e-8db3-72918328632d",
            "LongExpression": {
              "name": "StringAttribute_fcba2ffb-414f-485e-8db3-72918328632d_Value"
            },
            "Id": {
              "name": "StringAttribute_fcba2ffb-414f-485e-8db3-72918328632d_Id"
            }
          },
          {
            "labelId": "f9455688-ee78-41ec-bd56-0d67d6d485e8",
            "LongExpression": {
              "name": "StringAttribute_f9455688-ee78-41ec-bd56-0d67d6d485e8_Value"
            },
            "Id": {
              "name": "StringAttribute_f9455688-ee78-41ec-bd56-0d67d6d485e8_Id"
            }
          }
        ],
        "SingleValueListAttribute": [
          {
            "labelId": "cef22d91-5c49-4476-aebc-368a755e6f3e",
            "Value": {
              "name": "SingleValueListAttribute_cef22d91-5c49-4476-aebc-368a755e6f3e_Value"
            },
            "Id": {
              "name": "SingleValueListAttribute_cef22d91-5c49-4476-aebc-368a755e6f3e_Id"
            }
          },
          {
            "labelId": "066f514f-36c9-4c7e-8154-c304f65357ae",
            "Value": {
              "name": "SingleValueListAttribute_066f514f-36c9-4c7e-8154-c304f65357ae_Value"
            },
            "Id": {
              "name": "SingleValueListAttribute_066f514f-36c9-4c7e-8154-c304f65357ae_Id"
            }
          },
          {
            "labelId": "11d3a60c-6c03-45d1-b34f-d68159c60a39",
            "Value": {
              "name": "SingleValueListAttribute_11d3a60c-6c03-45d1-b34f-d68159c60a39_Value"
            },
            "Id": {
              "name": "SingleValueListAttribute_11d3a60c-6c03-45d1-b34f-d68159c60a39_Id"
            }
          },
          {
            "labelId": "034715d3-1480-4de7-b564-091bf770857b",
            "Value": {
              "name": "SingleValueListAttribute_034715d3-1480-4de7-b564-091bf770857b_Value"
            },
            "Id": {
              "name": "SingleValueListAttribute_034715d3-1480-4de7-b564-091bf770857b_Id"
            }
          }
        ],
        "Filter": {
          "AND": [
            {
              "Field": {
                "name": "AssetType_Name",
                "operator": "EQUALS",
                "value": "Data Attribute"
              }
            }
          ]
        },
        "Order": [
          {
            "Field": {
              "name": "TargetRelation_00000000-0000-0000-0000-000000007047_Source_FullName",
              "order": "ASC"
            }
          }
        ]
      }
    },
    "Columns": [
      {
        "Group": {
          "name": "TargetRelation_00000000-0000-0000-0000-000000007047",
          "Columns": [
            {
              "Column": {
                "fieldName": "TargetRelation_00000000-0000-0000-0000-000000007047_Source_FullName",
                "label": "Data_Entity_Name"
              }
            }
          ]
        }
      },
      {
        "Group": {
          "name": "SourceRelation_00000000-0000-0000-0000-000000007094",
          "Columns": [
            {
              "Column": {
                "fieldName": "SourceRelation_00000000-0000-0000-0000-000000007094_Target_DisplayName",
                "label": "Data_Attribute_Name"
              }
            }
          ]
        }
      },
      {
        "Group": {
          "name": "SingleValueListAttribute_cef22d91-5c49-4476-aebc-368a755e6f3e",
          "Columns": [
            {
              "Column": {
                "fieldName": "SingleValueListAttribute_cef22d91-5c49-4476-aebc-368a755e6f3e_Value",
                "label": "Treatment_In_Transit"
              }
            }
          ]
        }
      },
      {
        "Group": {
          "name": "SingleValueListAttribute_066f514f-36c9-4c7e-8154-c304f65357ae",
          "Columns": [
            {
              "Column": {
                "fieldName": "SingleValueListAttribute_066f514f-36c9-4c7e-8154-c304f65357ae_Value",
                "label": "Treatment_At_Rest"
              }
            }
          ]
        }
      },
      {
        "Group": {
          "name": "StringAttribute_cfbb6bf5-00f1-4bc4-ac66-8bb9fa44245e",
          "Columns": [
            {
              "Column": {
                "fieldName": "StringAttribute_cfbb6bf5-00f1-4bc4-ac66-8bb9fa44245e_Value",
                "label": "Cross_Border_Data_Treatment"
              }
            }
          ]
        }
      },

      {
        "Column": {
          "fieldName": "AssetType_Name",
          "label": "Asset_Type"
        }
      },
    ]
  }
}
