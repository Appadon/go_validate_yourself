package service

import (
	"context"
	"fmt"
	"strings"

	"go_validate_yourself/internal/batchparquet"
	"go_validate_yourself/internal/progress"
	"go_validate_yourself/internal/splitcsv"
)

/*
PipelinePhase identifies one executable service phase in a resolved
pipeline.
*/
type PipelinePhase string

const (
	PipelinePhaseSplit    PipelinePhase = "split"
	PipelinePhaseValidate PipelinePhase = "validate"
	PipelinePhaseBatch    PipelinePhase = "batch"
)

/*
PipelineResumePolicy describes how reusable phase outputs should be
handled by the pipeline runner.
*/
type PipelineResumePolicy string

const (
	PipelineResumePolicyRunAll              PipelineResumePolicy = "run_all"
	PipelineResumePolicyReuseValidOutputs   PipelineResumePolicy = "reuse_valid_outputs"
	PipelineResumePolicyStartAtFirstMissing PipelineResumePolicy = "start_at_first_missing"
)

/*
PipelineOptions defines a fully resolved service-level pipeline run.

The caller owns config parsing and CLI/API overlay behavior. The service
only receives ordered phases and typed phase execution options.
*/
type PipelineOptions struct {
	Phases                    []PipelinePhase
	ResumePolicy              PipelineResumePolicy
	Split                     SplitOptions
	Validate                  ValidateOptions
	Batch                     BatchOptions
	ReuseSplitCache           bool
	ClearValidationOutputDirs bool
	RunID                     string
	Reporter                  progress.Reporter
	Mode                      string
}

/*
PipelineResult records the phases processed by RunPipeline and the
phase-specific results that were produced before completion or failure.
*/
type PipelineResult struct {
	Phases          []PipelinePhase            `json:"phases"`
	RanPhases       []PipelinePhase            `json:"ran_phases"`
	SplitPrimaryKey string                     `json:"split_primary_key,omitempty"`
	SplitReused     bool                       `json:"split_reused"`
	SplitSummary    splitcsv.Summary           `json:"split_summary"`
	ValidationFile  *ValidationResult          `json:"validation_file,omitempty"`
	ValidationDir   *DirectoryValidationResult `json:"validation_dir,omitempty"`
	BatchSummary    batchparquet.Summary       `json:"batch_summary"`
}

/* RunPipeline executes a resolved pipeline phase by phase. */
func (s Service) RunPipeline(ctx context.Context, opts PipelineOptions) (PipelineResult, error) {
	ctx = ensureContext(ctx)
	normalized, err := normalizePipelineOptions(opts)
	if err != nil {
		return PipelineResult{}, err
	}

	emitter := progress.NewEmitter(normalized.RunID, normalized.Reporter)
	mode := pipelineMode(normalized)
	emitter.Started(progress.PhaseRun, "validation run started", map[string]any{
		"mode": mode,
	})

	if normalized.ClearValidationOutputDirs {
		emitter.Log(
			progress.PhaseRun,
			fmt.Sprintf("clearing validation cache directories: %s, %s, %s",
				normalized.Validate.SuccessDir,
				normalized.Validate.ErrorDir,
				normalized.Batch.OutputDir,
			), map[string]any{
				"success_dir":      normalized.Validate.SuccessDir,
				"error_dir":        normalized.Validate.ErrorDir,
				"batch_export_dir": normalized.Batch.OutputDir,
			},
		)
		emitter.Log(progress.PhaseRun, "this might take a while depending on the size of the cache", nil)
		if err := clearValidationOutputDirs(normalized.Validate.SuccessDir, normalized.Validate.ErrorDir, normalized.Batch.OutputDir); err != nil {
			emitter.Failed(progress.PhaseRun, err.Error(), nil)
			return PipelineResult{Phases: append([]PipelinePhase(nil), normalized.Phases...)}, err
		}
	}
	if err := ctx.Err(); err != nil {
		emitter.Failed(progress.PhaseRun, err.Error(), nil)
		return PipelineResult{Phases: append([]PipelinePhase(nil), normalized.Phases...)}, err
	}

	result := PipelineResult{
		Phases: append([]PipelinePhase(nil), normalized.Phases...),
	}
	for _, phase := range normalized.Phases {
		switch phase {
		case PipelinePhaseSplit:
			if err := s.runPipelineSplit(ctx, normalized, emitter, &result); err != nil {
				emitter.Failed(progress.PhaseRun, err.Error(), nil)
				return result, err
			}
		case PipelinePhaseValidate:
			if err := runPipelineValidate(ctx, normalized, emitter, &result); err != nil {
				emitter.Failed(progress.PhaseRun, err.Error(), nil)
				return result, err
			}
		case PipelinePhaseBatch:
			if err := runPipelineBatch(ctx, normalized, emitter, &result); err != nil {
				emitter.Failed(progress.PhaseRun, err.Error(), nil)
				return result, err
			}
		default:
			return result, fmt.Errorf("unsupported pipeline phase %q", phase)
		}
	}

	emitter.Completed(progress.PhaseRun, "validation run complete", pipelineCompletionMetrics(mode, result))
	return result, nil
}

/*
normalizePipelineOptions validates supported phase order and applies
phase-derived defaults between adjacent service options.
*/
func normalizePipelineOptions(opts PipelineOptions) (PipelineOptions, error) {
	normalized := opts
	if strings.TrimSpace(string(normalized.ResumePolicy)) == "" {
		normalized.ResumePolicy = PipelineResumePolicyRunAll
	}
	if err := validatePipelineResumePolicy(normalized.ResumePolicy); err != nil {
		return PipelineOptions{}, err
	}

	normalized.Phases = append([]PipelinePhase(nil), normalized.Phases...)
	for i, phase := range normalized.Phases {
		normalized.Phases[i] = PipelinePhase(strings.ToLower(strings.TrimSpace(string(phase))))
	}
	if err := validatePipelineSequence(normalized.Phases); err != nil {
		return PipelineOptions{}, err
	}

	if phaseBefore(normalized.Phases, PipelinePhaseSplit, PipelinePhaseValidate) &&
		strings.TrimSpace(normalized.Validate.InputCSV) == "" &&
		strings.TrimSpace(normalized.Validate.InputDir) == "" {
		normalized.Validate.InputDir = normalized.Split.OutputDir
	}
	if phaseBefore(normalized.Phases, PipelinePhaseValidate, PipelinePhaseBatch) &&
		strings.TrimSpace(normalized.Batch.InputDir) == "" {
		normalized.Batch.InputDir = normalized.Validate.SuccessDir
	}

	if err := validatePipelineInputs(normalized); err != nil {
		return PipelineOptions{}, err
	}
	return normalized, nil
}

func validatePipelineResumePolicy(policy PipelineResumePolicy) error {
	switch policy {
	case PipelineResumePolicyRunAll, PipelineResumePolicyReuseValidOutputs, PipelineResumePolicyStartAtFirstMissing:
		return nil
	default:
		return fmt.Errorf("unsupported pipeline resume policy %q", policy)
	}
}

func validatePipelineSequence(phases []PipelinePhase) error {
	switch {
	case pipelinePhasesEqual(phases, []PipelinePhase{PipelinePhaseSplit}),
		pipelinePhasesEqual(phases, []PipelinePhase{PipelinePhaseValidate}),
		pipelinePhasesEqual(phases, []PipelinePhase{PipelinePhaseBatch}),
		pipelinePhasesEqual(phases, []PipelinePhase{PipelinePhaseSplit, PipelinePhaseValidate}),
		pipelinePhasesEqual(phases, []PipelinePhase{PipelinePhaseValidate, PipelinePhaseBatch}),
		pipelinePhasesEqual(phases, []PipelinePhase{PipelinePhaseSplit, PipelinePhaseValidate, PipelinePhaseBatch}):
		return nil
	default:
		return fmt.Errorf("unsupported pipeline phase sequence %v", phases)
	}
}

func validatePipelineInputs(opts PipelineOptions) error {
	for _, phase := range opts.Phases {
		switch phase {
		case PipelinePhaseSplit:
			if strings.TrimSpace(opts.Split.InputPath) == "" {
				return fmt.Errorf("split phase requires input path")
			}
			if strings.TrimSpace(opts.Split.OutputDir) == "" {
				return fmt.Errorf("split phase requires output dir")
			}
		case PipelinePhaseValidate:
			if strings.TrimSpace(opts.Validate.SchemaPath) == "" {
				return fmt.Errorf("validate phase requires schema path")
			}
			if strings.TrimSpace(opts.Validate.InputCSV) == "" && strings.TrimSpace(opts.Validate.InputDir) == "" {
				return fmt.Errorf("validate phase requires input csv, input dir, or a prior split phase")
			}
			if strings.TrimSpace(opts.Validate.SuccessDir) == "" {
				return fmt.Errorf("validate phase requires success dir")
			}
			if strings.TrimSpace(opts.Validate.ErrorDir) == "" {
				return fmt.Errorf("validate phase requires error dir")
			}
		case PipelinePhaseBatch:
			if strings.TrimSpace(opts.Batch.InputDir) == "" {
				return fmt.Errorf("batch phase requires input dir or a prior validate phase")
			}
			if strings.TrimSpace(opts.Batch.OutputDir) == "" {
				return fmt.Errorf("batch phase requires output dir")
			}
		}
	}
	if opts.ClearValidationOutputDirs && containsPipelinePhase(opts.Phases, PipelinePhaseValidate) && strings.TrimSpace(opts.Batch.OutputDir) == "" {
		return fmt.Errorf("clear validation output requires batch output dir")
	}
	return nil
}

func (s Service) runPipelineSplit(ctx context.Context, opts PipelineOptions, emitter progress.Emitter, result *PipelineResult) error {
	primaryKey := strings.TrimSpace(opts.Split.PrimaryKey)
	if primaryKey == "" {
		detected, err := DetectPrimaryKey(opts.Split.InputPath)
		if err != nil {
			return fmt.Errorf("failed detecting split primary key: %w", err)
		}
		primaryKey = detected
	}
	result.SplitPrimaryKey = primaryKey

	if shouldReuseSplitCache(opts) {
		summary, reused, err := s.prepareAutoSplit(ctx, AutoOptions{
			MainInputCSV:     opts.Split.InputPath,
			SplitOutputDir:   opts.Split.OutputDir,
			SplitMaxOpen:     opts.Split.MaxOpenWriters,
			SplitMissingFile: opts.Split.MissingKeysFile,
		}, primaryKey, emitter)
		if err != nil {
			return err
		}
		result.SplitSummary = summary
		result.SplitReused = reused
		result.RanPhases = append(result.RanPhases, PipelinePhaseSplit)
		return nil
	}

	summary, err := runSplitPhase(ctx, SplitOptions{
		InputPath:       opts.Split.InputPath,
		OutputDir:       opts.Split.OutputDir,
		PrimaryKey:      primaryKey,
		MaxOpenWriters:  opts.Split.MaxOpenWriters,
		MissingKeysFile: opts.Split.MissingKeysFile,
	}, emitter)
	if err != nil {
		return err
	}
	result.SplitSummary = summary
	result.RanPhases = append(result.RanPhases, PipelinePhaseSplit)
	return nil
}

func runPipelineValidate(ctx context.Context, opts PipelineOptions, emitter progress.Emitter, result *PipelineResult) error {
	if strings.TrimSpace(opts.Validate.InputCSV) != "" {
		validationResult, err := runValidateFilePhase(ctx, opts.Validate, emitter)
		if err != nil {
			if validationResult.InputPath != "" {
				result.ValidationFile = &validationResult
			}
			return err
		}
		result.ValidationFile = &validationResult
		result.RanPhases = append(result.RanPhases, PipelinePhaseValidate)
		return nil
	}

	validationResult, err := runValidateDirPhase(ctx, opts.Validate, emitter)
	if validationResult.InputDir != "" {
		result.ValidationDir = &validationResult
	}
	if err != nil {
		return err
	}
	result.RanPhases = append(result.RanPhases, PipelinePhaseValidate)
	return nil
}

func runPipelineBatch(ctx context.Context, opts PipelineOptions, emitter progress.Emitter, result *PipelineResult) error {
	batchSummary, err := runBatchPhase(ctx, opts.Batch, emitter)
	if err != nil {
		if batchSummary.OutputDir != "" {
			result.BatchSummary = batchSummary
		}
		return err
	}
	result.BatchSummary = batchSummary
	result.RanPhases = append(result.RanPhases, PipelinePhaseBatch)
	return nil
}

func shouldReuseSplitCache(opts PipelineOptions) bool {
	if !opts.ReuseSplitCache {
		return false
	}
	switch opts.ResumePolicy {
	case PipelineResumePolicyReuseValidOutputs, PipelineResumePolicyStartAtFirstMissing:
		return true
	default:
		return false
	}
}

func pipelineMode(opts PipelineOptions) string {
	if mode := strings.TrimSpace(opts.Mode); mode != "" {
		return mode
	}
	switch {
	case pipelinePhasesEqual(opts.Phases, []PipelinePhase{PipelinePhaseSplit}):
		return "split"
	case pipelinePhasesEqual(opts.Phases, []PipelinePhase{PipelinePhaseValidate}):
		if strings.TrimSpace(opts.Validate.InputCSV) != "" {
			return "validate-file"
		}
		return "validate-dir"
	case pipelinePhasesEqual(opts.Phases, []PipelinePhase{PipelinePhaseBatch}):
		return "batch"
	case pipelinePhasesEqual(opts.Phases, []PipelinePhase{PipelinePhaseSplit, PipelinePhaseValidate, PipelinePhaseBatch}):
		return "auto"
	default:
		return "pipeline"
	}
}

func pipelineCompletionMetrics(mode string, result PipelineResult) map[string]any {
	metrics := map[string]any{
		"mode": mode,
	}
	if containsPipelinePhase(result.Phases, PipelinePhaseSplit) {
		metrics["split_reused"] = result.SplitReused
		metrics["split_rows"] = result.SplitSummary.TotalRows
	}
	if result.ValidationDir != nil {
		metrics["validated"] = result.ValidationDir.Summary.Files
	}
	if result.ValidationFile != nil {
		metrics["validated"] = 1
	}
	if containsPipelinePhase(result.Phases, PipelinePhaseBatch) {
		metrics["batch_files"] = result.BatchSummary.InputFiles
		metrics["batch_outputs"] = result.BatchSummary.Batches
	}
	return metrics
}

func pipelinePhasesEqual(actual, expected []PipelinePhase) bool {
	if len(actual) != len(expected) {
		return false
	}
	for i := range actual {
		if actual[i] != expected[i] {
			return false
		}
	}
	return true
}

func containsPipelinePhase(phases []PipelinePhase, target PipelinePhase) bool {
	for _, phase := range phases {
		if phase == target {
			return true
		}
	}
	return false
}

func phaseBefore(phases []PipelinePhase, prior, target PipelinePhase) bool {
	priorIndex := -1
	targetIndex := -1
	for i, phase := range phases {
		if phase == prior {
			priorIndex = i
		}
		if phase == target {
			targetIndex = i
		}
	}
	return priorIndex >= 0 && targetIndex >= 0 && priorIndex < targetIndex
}
