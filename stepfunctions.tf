resource "aws_sfn_state_machine" "proximity_pipeline" {
  name     = "ProximityETLPipeline"
  role_arn = aws_iam_role.step_functions_role.arn

  definition = jsonencode({
    Comment = "State machine to orchestrate Glue ETL pipeline with SNS notifications"

    StartAt = "Extraction"
    States = {
      "Extraction" = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = "bronze-ingestion-job"
        }
        Next = "Crawl Bronze Data"
        Retry = [{
          ErrorEquals     = ["States.ALL"],
          IntervalSeconds = 30,
          MaxAttempts     = 3,
          BackoffRate     = 2
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"],
          ResultPath  = "$.error-info",
          Next        = "Notify Extraction Failure"
        }]
      }

      "Crawl Bronze Data" = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:glue:startCrawler"
        Parameters = {
          Name = "bronze-crawler"
        }
        Next = "Wait For Bronze Crawl"
        Retry = [{
          ErrorEquals     = ["States.ALL"],
          IntervalSeconds = 10,
          MaxAttempts     = 3,
          BackoffRate     = 2
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"],
          Next        = "Notify Crawler Failure"
        }]
      }

      "Wait For Bronze Crawl" = {
        Type    = "Wait"
        Seconds = 60
        Next    = "Get Bronze Crawler Status"
      }

      "Get Bronze Crawler Status" = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:glue:getCrawler"
        Parameters = {
          Name = "bronze-crawler"
        }
        Next = "Check Bronze Crawler Status"
      }

      "Check Bronze Crawler Status" = {
        Type = "Choice"
        Choices = [
          {
            Variable     = "$.Crawler.LastCrawl.Status"
            StringEquals = "SUCCEEDED"
            Next         = "Transform To Silver"
          },
          {
            Variable     = "$.Crawler.LastCrawl.Status"
            StringEquals = "FAILED"
            Next         = "Notify Crawler Failure"
          }
        ]
        Default = "Wait For Bronze Crawl"
      }

      "Transform To Silver" = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = "transformations-job"
        }
        Next = "Crawl Silver Data"
        Retry = [{
          ErrorEquals     = ["States.ALL"],
          IntervalSeconds = 30,
          MaxAttempts     = 3,
          BackoffRate     = 2
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"],
          Next        = "Notify Transformation Failure"
        }]
      }

      "Crawl Silver Data" = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:glue:startCrawler"
        Parameters = {
          Name = "silver-crawler"
        }
        Next = "Wait For Silver Crawl"
        Retry = [{
          ErrorEquals     = ["States.ALL"],
          IntervalSeconds = 10,
          MaxAttempts     = 3,
          BackoffRate     = 2
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"],
          Next        = "Notify Crawler Failure"
        }]
      }

      "Wait For Silver Crawl" = {
        Type    = "Wait"
        Seconds = 60
        Next    = "Get Silver Crawler Status"
      }

      "Get Silver Crawler Status" = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:glue:getCrawler"
        Parameters = {
          Name = "silver-crawler"
        }
        Next = "Check Silver Crawler Status"
      }

      "Check Silver Crawler Status" = {
        Type = "Choice"
        Choices = [
          {
            Variable     = "$.Crawler.LastCrawl.Status"
            StringEquals = "SUCCEEDED"
            Next         = "Curate Gold Layer"
          },
          {
            Variable     = "$.Crawler.LastCrawl.Status"
            StringEquals = "FAILED"
            Next         = "Notify Crawler Failure"
          }
        ]
        Default = "Wait For Silver Crawl"
      }

      "Curate Gold Layer" = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = "gold-data-curation-job"
        }
        Next = "Crawl Gold Data"
        Catch = [{
          ErrorEquals = ["States.ALL"],
          Next        = "Notify Curation Failure"
        }]
      }

      "Crawl Gold Data" = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:glue:startCrawler"
        Parameters = {
          Name = "gold-crawler"
        }
        Next = "Wait For Gold Crawl"
        Retry = [{
          ErrorEquals     = ["States.ALL"],
          IntervalSeconds = 10,
          MaxAttempts     = 3,
          BackoffRate     = 2
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"],
          Next        = "Notify Crawler Failure"
        }]
      }

      "Wait For Gold Crawl" = {
        Type    = "Wait"
        Seconds = 60
        Next    = "Get Gold Crawler Status"
      }

      "Get Gold Crawler Status" = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:glue:getCrawler"
        Parameters = {
          Name = "gold-crawler"
        }
        Next = "Check Gold Crawler Status"
      }

      "Check Gold Crawler Status" = {
        Type = "Choice"
        Choices = [
          {
            Variable     = "$.Crawler.LastCrawl.Status"
            StringEquals = "SUCCEEDED"
            Next         = "Load To Redshift"
          },
          {
            Variable     = "$.Crawler.LastCrawl.Status"
            StringEquals = "FAILED"
            Next         = "Notify Crawler Failure"
          }
        ]
        Default = "Wait For Gold Crawl"
      }

      "Load To Redshift" = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = "s3-to-redshift-job"
        }
        Next = "Notify Success"
        Catch = [{
          ErrorEquals = ["States.ALL"],
          Next        = "Notify Redshift Failure"
        }]
      }

      "Notify Extraction Failure" = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn = aws_sns_topic.notifications.arn
          Message  = "❌ RDS to Bronze ingestion job failed."
          Subject  = "Step Function Alert: Extraction Failed"
        }
        End = true
      }

      "Notify Transformation Failure" = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn = aws_sns_topic.notifications.arn
          Message  = "❌ Bronze to Silver transformation job failed."
          Subject  = "Step Function Alert: Transformation Failed"
        }
        End = true
      }

      "Notify Curation Failure" = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn = aws_sns_topic.notifications.arn
          Message  = "❌ Silver to Gold curation job failed."
          Subject  = "Step Function Alert: Gold Curation Failed"
        }
        End = true
      }

      "Notify Redshift Failure" = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn = aws_sns_topic.notifications.arn
          Message  = "❌ Redshift loading job failed."
          Subject  = "Step Function Alert: Redshift Load Failed"
        }
        End = true
      }

      "Notify Crawler Failure" = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn = aws_sns_topic.notifications.arn
          Message  = "❌ One of the crawlers failed."
          Subject  = "Step Function Alert: Crawler Failed"
        }
        End = true
      }

      "Notify Success" = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn = aws_sns_topic.notifications.arn
          Message  = "✅ Data pipeline successfully loaded into Redshift."
          Subject  = "Step Function Success"
        }
        End = true
      }
    }
  })
}
