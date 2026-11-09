variable "region" {
  type    = string
  default = "us-east-1"
}

variable "name_suffix" {
  description = "Short name that keeps several setups apart, e.g. a city."
  type        = string
  default     = "demo"
}

variable "raw_retention_days" {
  description = "Days to keep raw positions in S3."
  type        = number
  default     = 30
}

variable "allow_destroy_with_data" {
  description = "Let `terraform destroy` delete the bucket even when it still holds files."
  type        = bool
  default     = false
}

variable "budget_email" {
  description = "Email for the budget alert. Empty creates no alert."
  type        = string
  default     = ""
}

variable "monthly_budget_usd" {
  type    = number
  default = 5
}
