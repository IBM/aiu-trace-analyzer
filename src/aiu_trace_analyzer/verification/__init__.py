# Copyright 2024-2026 IBM Corporation

'''
Verification pipeline stages and contexts.

The stages of the verification pipeline are collected here, separate from the regular processing
pipeline of aiu_trace_analyzer.pipeline. The dependency between the two is one-directional:
verification stages build on the pipeline infrastructure (contexts, detection algorithms), the
regular pipeline never imports anything from here.

A verification stage consists of a context derived from AbstractVerificationContext (which emits
the accumulated findings as verification meta-data events at drain time) plus one or more callback
functions. For a minimal example of both, see verify.py.
'''

# import the verification contexts:
from aiu_trace_analyzer.verification.verify import VerificationContext
from aiu_trace_analyzer.verification.kernel_parent_verify import KernelParentVerificationContext
from aiu_trace_analyzer.verification.overlap_verify import OverlapVerificationContext

# import the verification stage callbacks:
from aiu_trace_analyzer.verification.verify import verify, verify_cleanup
from aiu_trace_analyzer.verification.kernel_parent_verify import (
    kernel_parent_collect,
    kernel_parent_verify
)
from aiu_trace_analyzer.verification.overlap_verify import verify_kernel_overlap
from aiu_trace_analyzer.verification.report import verification_result_filter
