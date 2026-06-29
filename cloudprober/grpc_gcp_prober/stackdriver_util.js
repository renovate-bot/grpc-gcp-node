const {ErrorReporting} = require('@google-cloud/error-reporting');

/** Utility class for collection Stackdriver metrics and errors. */
class StackdriverUtil {
  /**
   * Constructor of StackdriverUtil
   * @param {string} api The cloud api name of the metrics being generated.
   */
  constructor(api) {
    this.api_ = api;
    this.metircs_ = {};
    this.success_ = false;
    this.errClient_ = new ErrorReporting();
  }

  /**
   * Add a map of metrics to the existing metrics.
   * @param {Object} metrics A map of metrics.
   */
  addMetrics(metrics) {
    this.metrics_ = Object.assign({}, this.metrics_, metrics);
  }

  /**
   * Set the result of the probe.
   * @param {boolean} result
   */
  setSuccess(result) {
    this.success_ = result;
  }

  /**
   * Format output before they can be made to Stackdriver metrics.
   *
   * Formatted output like 'metric<space>value' will be scraped by Stackdriver
   * as custom metrics.
   */
  outputMetrics() {
    if (this.success_) {
      console.log(this.api_ + '_success 1');
    } else {
      console.log(this.api_ + '_success 0');
    }

    for (const metric_name in this.metrics_) {
      console.log(metric_name + ' ' + this.metrics_[metric_name]);
    }
  }

  /**
   * Format error message to output to error reporting.
   */
  reportError(err) {
    console.error(err);
    this.errClient_.report(
      'NodeProberFailure: gRPC(v=x.x.x) fails on ' +
        this.api_ +
        ' API. Details: ' +
        err.toString()
    );
  }
}

exports.StackdriverUtil = StackdriverUtil;
