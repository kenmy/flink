package org.apache.flink.runtime.jobmanager;

import org.apache.flink.api.common.JobSubmissionResult;

/**
 * Created by Evgeny_Kincharov on 1/13/2017.
 */
public interface OnSubmitActions {

	void jobSubmitted(JobSubmissionResult result);

	JobSubmissionResult getJobSubmissionResult();

}
