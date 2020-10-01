#!/usr/bin/env node

'use strict'

const meow = require('meow')
const assert = require('assert')
const Moment = require('moment');
const MomentRange = require('moment-range');
const Writable = require('stream').Writable
const Readable = require('stream').Readable
const Throttled = require('throttled-transform-stream').default
const bole = require('bole')

const moment = MomentRange.extendMoment(Moment);

const toS3 = require('./')

const datePlaceholder = /{{date}}/gi

const copy = function(d) {
  return new Promise((resolve, reject) => {
      const prefix = options.azure.prefix.replace(datePlaceholder, d)
      toS3.log.s3.info({ message: 'directory', prefix })
      toS3({
          concurrency: options.concurrency,
            azure: {
              connection: options.azure.connection,
              container: options.azure.container,
              prefix: prefix
            },
            aws: {
              region: options.aws.region,
              bucket: options.aws.bucket
            }
          }).pipe(new Writable({
              write: function (file, enc, callback) {
                toS3.log.s3.info({ message: 'file', file })
                callback()
              },
              objectMode: true
          }))
          .on('end', resolve)
          .on('close', resolve)
          .on('error', (err) => {
            toS3.log.s3.error({ message: 'error', err })
            reject(err)
          })
      })
}

const main = async function(dates) {
    for (const i in dates) {
        const d = dates[i]
        await copy(d)
    }
}

const cli = meow(`
  Usage:
    azure-s3
      Copies an Azure Blob Storage IT novem container's files to an visucad Amazon S3 bucket.

  Options:
    --concurrency
    --log-level
    --begin-date
    --end-date
    --azure-prefix           Azure prefix ({{date}} can be put in prefix and will be replace with the given date range)
    --azure-connection       Azure Blob Storage connection string
    --azure-container        Azure Blob Storage container name
    --azure-prefix           Azure Blob Storage blob prefix
    --aws-bucket             AWS S3 bucket name
    --aws-region             AWS region for the bucket
    --aws-access-key-id      AWS IAM access key ID
    --aws-secret-access-key  AWS IAM access key secret
`)


const options = {
  concurrency: cli.flags.concurrency || 30,
  logLevel: cli.flags.logLevel,
  beginDate: cli.flags.beginDate,
  endDate: cli.flags.endDate,
  azure: {
    connection: cli.flags.azureConnection,
    container: cli.flags.azureContainer,
    token: cli.flags.azureToken,
    prefix: cli.flags.azurePrefix
  },
  aws: {
    bucket: cli.flags.awsBucket,
    region: cli.flags.awsRegion,
    accessKeyId: cli.flags.awsAccessKeyId,
    secretAccessKey: cli.flags.awsSecretAccessKey
  }
}

bole.output({
  level: options.logLevel || 'info',
  stream: process.stdout
})

assert(options.beginDate, 'begin date required')

const start = moment(options.beginDate, 'YYYY-MM-DD');
const end   = options.endDate ? moment(options.endDate, 'YYYY-MM-DD') : moment();
const range = moment.range(start, end);

toS3.log.s3.info({ message: 'dates', begin: start, end: end })

const dates = Array.from(range.by('days')).map(d => d.format('YYYYMMDD'))

main(dates)
