// Package api Code generated by swaggo/swag. DO NOT EDIT
package api

import "github.com/swaggo/swag"

const docTemplate = `{
    "schemes": {{ marshal .Schemes }},
    "swagger": "2.0",
    "info": {
        "description": "{{escape .Description}}",
        "title": "{{.Title}}",
        "contact": {
            "name": "ClusterCockpit Project",
            "url": "https://clustercockpit.org",
            "email": "support@clustercockpit.org"
        },
        "license": {
            "name": "MIT License",
            "url": "https://opensource.org/licenses/MIT"
        },
        "version": "{{.Version}}"
    },
    "host": "{{.Host}}",
    "basePath": "{{.BasePath}}",
    "paths": {
        "/query/": {
            "get": {
                "security": [
                    {
                        "ApiKeyAuth": []
                    }
                ],
                "description": "Query events.",
                "consumes": [
                    "application/json"
                ],
                "produces": [
                    "application/json"
                ],
                "tags": [
                    "query"
                ],
                "summary": "Query events",
                "parameters": [
                    {
                        "description": "API query payload object",
                        "name": "request",
                        "in": "body",
                        "required": true,
                        "schema": {
                            "$ref": "#/definitions/api.ApiQueryRequest"
                        }
                    }
                ],
                "responses": {
                    "200": {
                        "description": "API query response object",
                        "schema": {
                            "$ref": "#/definitions/api.ApiQueryResponse"
                        }
                    },
                    "400": {
                        "description": "Bad Request",
                        "schema": {
                            "$ref": "#/definitions/api.ErrorResponse"
                        }
                    },
                    "401": {
                        "description": "Unauthorized",
                        "schema": {
                            "$ref": "#/definitions/api.ErrorResponse"
                        }
                    },
                    "500": {
                        "description": "Internal Server Error",
                        "schema": {
                            "$ref": "#/definitions/api.ErrorResponse"
                        }
                    }
                }
            }
        },
        "/write/": {
            "post": {
                "security": [
                    {
                        "ApiKeyAuth": []
                    }
                ],
                "description": "Receive events in line-protocol",
                "consumes": [
                    "text/plain"
                ],
                "produces": [
                    "application/json"
                ],
                "tags": [
                    "write"
                ],
                "summary": "Receive events",
                "parameters": [
                    {
                        "type": "string",
                        "description": "If the lines in the body do not have a cluster tag, use this value instead.",
                        "name": "cluster",
                        "in": "query",
                        "required": true
                    }
                ],
                "responses": {
                    "200": {
                        "description": "ok",
                        "schema": {
                            "type": "string"
                        }
                    },
                    "400": {
                        "description": "Bad Request",
                        "schema": {
                            "$ref": "#/definitions/api.ErrorResponse"
                        }
                    },
                    "401": {
                        "description": "Unauthorized",
                        "schema": {
                            "$ref": "#/definitions/api.ErrorResponse"
                        }
                    },
                    "500": {
                        "description": "Internal Server Error",
                        "schema": {
                            "$ref": "#/definitions/api.ErrorResponse"
                        }
                    }
                }
            }
        }
    },
    "definitions": {
        "api.ApiMetricData": {
            "type": "object",
            "properties": {
                "data": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    }
                },
                "error": {
                    "type": "string"
                },
                "from": {
                    "type": "integer"
                },
                "to": {
                    "type": "integer"
                }
            }
        },
        "api.ApiQuery": {
            "type": "object",
            "properties": {
                "event": {
                    "type": "string"
                },
                "host": {
                    "type": "string"
                },
                "subtype": {
                    "type": "string"
                },
                "subtype-ids": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    }
                },
                "type": {
                    "type": "string"
                },
                "type-ids": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    }
                }
            }
        },
        "api.ApiQueryRequest": {
            "type": "object",
            "properties": {
                "cluster": {
                    "type": "string"
                },
                "for-all-nodes": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    }
                },
                "from": {
                    "type": "integer"
                },
                "queries": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/api.ApiQuery"
                    }
                },
                "to": {
                    "type": "integer"
                }
            }
        },
        "api.ApiQueryResponse": {
            "type": "object",
            "properties": {
                "queries": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/api.ApiQuery"
                    }
                },
                "results": {
                    "type": "array",
                    "items": {
                        "type": "array",
                        "items": {
                            "$ref": "#/definitions/api.ApiMetricData"
                        }
                    }
                }
            }
        },
        "api.ErrorResponse": {
            "type": "object",
            "properties": {
                "error": {
                    "description": "Error Message",
                    "type": "string"
                },
                "status": {
                    "description": "Statustext of Errorcode",
                    "type": "string"
                }
            }
        }
    },
    "securityDefinitions": {
        "ApiKeyAuth": {
            "type": "apiKey",
            "name": "X-Auth-Token",
            "in": "header"
        }
    }
}`

// SwaggerInfo holds exported Swagger Info so clients can modify it
var SwaggerInfo = &swag.Spec{
	Version:          "1.0.0",
	Host:             "localhost:8098",
	BasePath:         "/api/",
	Schemes:          []string{},
	Title:            "cc-event-store REST API",
	Description:      "API for cc-event-store",
	InfoInstanceName: "swagger",
	SwaggerTemplate:  docTemplate,
	LeftDelim:        "{{",
	RightDelim:       "}}",
}

func init() {
	swag.Register(SwaggerInfo.InstanceName(), SwaggerInfo)
}