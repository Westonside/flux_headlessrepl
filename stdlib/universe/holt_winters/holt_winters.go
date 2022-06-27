package holt_winters

import (
	"math"

	"github.com/apache/arrow/go/v7/arrow/memory"
	"github.com/influxdata/flux/array"
	"github.com/influxdata/flux/arrow"
	"github.com/influxdata/flux/internal/mutable"
	"gonum.org/v1/gonum/optimize"
)

// HoltWinters forecasts a series into the future.
// This is done using the Holt-Winters damped method.
//    1. The initial values are calculated using a SSE.
//    2. The series is forecast into the future using the iterative relations.
type HoltWinters struct {
	n              int
	s              int
	seasonal       bool
	includeFitData bool
	// NelderMead optimizer
	optim *Optimizer
	// Small difference bound for the optimizer
	epsilon float64

	vs    *array.Float
	alloc memory.Allocator
}

const (
	// Arbitrary weight for initializing some intial guesses.
	// This should be in the  range [0,1]
	hwWeight = 0.5
	// Epsilon value for the minimization process
	hwDefaultEpsilon = 1.0e-4
	// Define a grid of initial guesses for the parameters: alpha, beta, gamma, and phi.
	// Keep in mind that this grid is N^4 so we should keep N small
	// The starting lower guess
	hwGuessLower = 0.3
	//  The upper bound on the grid
	hwGuessUpper = 1.0
	// The step between guesses
	hwGuessStep = 0.4
)

// New creates a new HoltWinters.
// HoltWinters uses the given allocator for memory tracking purposes,
// and in order to build its result.
func New(n, s int, withFit bool, alloc memory.Allocator) *HoltWinters {
	seasonal := s >= 2
	return &HoltWinters{
		n:              n,
		s:              s,
		seasonal:       seasonal,
		includeFitData: withFit,
		optim:          NewOptimizer(alloc),
		epsilon:        hwDefaultEpsilon,
		alloc:          alloc,
	}
}

// Do returns the points generated by the HoltWinters algorithm given a dataset.
func (r *HoltWinters) Do(vs *array.Float) *array.Float {
	r.vs = vs
	l := vs.Len() // l is the length of both times and values
	if l < 2 || r.seasonal && l < r.s || r.n <= 0 {
		return arrow.NewFloat(nil, nil)
	}
	m := r.s

	// Starting guesses
	// NOTE: Since these values are guesses
	// in the cases where we were missing data,
	// we can just skip the value and call it good.
	l0 := 0.0
	if r.seasonal {
		for i := 0; i < m; i++ {
			if vs.IsValid(i) {
				l0 += (1 / float64(m)) * vs.Value(i)
			}
		}
	} else {
		l0 += hwWeight * vs.Value(0)
	}

	b0 := 0.0
	if r.seasonal {
		for i := 0; i < m && m+i < vs.Len(); i++ {
			if vs.IsValid(i) && vs.IsValid(m+i) {
				b0 += 1 / float64(m*m) * (vs.Value(m+i) - vs.Value(i))
			}
		}
	} else {
		if vs.IsValid(1) {
			b0 = hwWeight * (vs.Value(1) - vs.Value(0))
		}
	}

	size := 6
	if r.seasonal {
		size += m
	}
	// These parameters will be used by the Optimizer to generate new parameters
	// basing on the `sse` function and changing alpha, beta, gamma, and phi.
	// As such, they will be used over and over in the optimization loop above, and they can only
	// be released at the end of this function.
	initParams := mutable.NewFloat64Array(r.alloc)
	defer initParams.Release()
	initParams.Resize(size)
	initParams.Set(4, l0)
	initParams.Set(5, b0)
	if r.seasonal {
		for i := 0; i < m; i++ {
			if vs.IsValid(i) {
				initParams.Set(i+6, vs.Value(i)/l0)
			} else {
				initParams.Set(i+6, 0)
			}
		}
	}

	// Determine best fit for the various parameters
	minSSE := math.Inf(1)
	var bestParams []float64

	// Params for gonum optimize
	f := mutable.NewFloat64Array(r.alloc)
	f.Resize(size)
	defer f.Release()
	problem := optimize.Problem{
		Func: func(par []float64) float64 {
			for i := 0; i < len(par); i++ {
				f.Set(i, par[i])
			}
			return r.sse(f)
		},
	}
	settings := optimize.Settings{Converger: &optimize.FunctionConverge{Absolute: 1e-10, Iterations: 100}}

	for alpha := hwGuessLower; alpha < hwGuessUpper; alpha += hwGuessStep {
		for beta := hwGuessLower; beta < hwGuessUpper; beta += hwGuessStep {
			for gamma := hwGuessLower; gamma < hwGuessUpper; gamma += hwGuessStep {
				for phi := hwGuessLower; phi < hwGuessUpper; phi += hwGuessStep {
					initParams.Set(0, alpha)
					initParams.Set(1, beta)
					initParams.Set(2, gamma)
					initParams.Set(3, phi)

					// Minimize creates new parameters every time it is called.
					result, err := optimize.Minimize(problem, initParams.Float64Values(), &settings, &optimize.NelderMead{})
					if err != nil {
						panic(err)
					}

					if result.F < minSSE || bestParams == nil {
						minSSE = result.F
						bestParams = result.X
					}
				}
			}
		}
	}

	// Final forecast
	bestParamsF := mutable.NewFloat64Array(r.alloc)
	bestParamsF.Resize(size)
	defer bestParamsF.Release()
	for i := 0; i < len(bestParams); i++ {
		bestParamsF.Set(i, bestParams[i])
	}
	fcast := r.forecast(bestParamsF, false)

	return fcast.NewFloat64Array()
}

// Using the recursive relations compute the next values
func (r *HoltWinters) next(alpha, beta, gamma, phi, phiH, yT, lTp, bTp, sTm, sTmh float64) (yTh, lT, bT, sT float64) {
	lT = alpha*(yT/sTm) + (1-alpha)*(lTp+phi*bTp)
	bT = beta*(lT-lTp) + (1-beta)*phi*bTp
	sT = gamma*(yT/(lTp+phi*bTp)) + (1-gamma)*sTm
	yTh = (lT + phiH*bT) * sTmh
	return
}

// Forecast the data.
// This method can be called either to predict `r.n` points in the future,
// or to get the current fit on the provided dataset.
// This can be chosen with the `onlyFit` flag.
// When predicting, it will use `r.includeFitData` to include the fit data or not.
// Forecast returns a new Float64Array, it is responsibility of the caller to Release it.
func (r *HoltWinters) forecast(params *mutable.Float64Array, onlyFit bool) *mutable.Float64Array {
	h := r.n
	if onlyFit {
		// no horizon if only fitting the dataset
		h = 0
	}
	// constrain parameters
	r.constrain(params)

	yT := r.vs.Value(0)

	phi := params.Value(3)
	phiH := phi

	lT := params.Value(4)
	bT := params.Value(5)

	// seasonals is a ring buffer of past sT values
	var m, so int
	const seasonalsStart = 6
	if r.seasonal {
		m = params.Len() - seasonalsStart
		if m == 1 {
			params.Set(seasonalsStart, 1)
		}
		// Season index offset
		so = m - 1
	}

	l := r.vs.Len()
	size := h
	if onlyFit || r.includeFitData {
		size += l
	}
	fcast := mutable.NewFloat64Array(r.alloc)
	fcast.Reserve(size)
	if onlyFit || r.includeFitData {
		fcast.Append(yT)
	}

	var hm int
	stm, stmh := 1.0, 1.0
	for t := 1; t < l+h; t++ {
		if r.seasonal {
			hm = t % m
			stm = params.Value(seasonalsStart + (t-m+so)%m)
			stmh = params.Value(seasonalsStart + (t-m+hm+so)%m)
		}
		var sT float64
		yT, lT, bT, sT = r.next(
			params.Value(0), // alpha
			params.Value(1), // beta
			params.Value(2), // gamma
			phi,
			phiH,
			yT,
			lT,
			bT,
			stm,
			stmh,
		)
		phiH += math.Pow(phi, float64(t))

		if r.seasonal {
			params.Set(seasonalsStart+(t+so)%m, sT)
			so++
		}

		if onlyFit || (r.includeFitData && t < l) || t >= l {
			fcast.Append(yT)
		}
	}
	return fcast
}

// Compute sum squared error for the given parameters.
func (r *HoltWinters) sse(params *mutable.Float64Array) float64 {
	sse := 0.0
	fcast := r.forecast(params, true)
	// These forecast values are used only to compute the sum of squares.
	// They can be released at the end of this function.
	defer fcast.Release()
	for i := 0; i < fcast.Len(); i++ {
		// Skip missing values since we cannot use them to compute an error.
		if r.vs.IsValid(i) {
			// Compute error
			if math.IsNaN(fcast.Value(i)) {
				// Penalize fcast NaNs
				return math.MaxFloat64
			}
			diff := fcast.Value(i) - r.vs.Value(i)
			sse += diff * diff
		}
	}
	return sse
}

// Constrain alpha, beta, gamma, phi in the range [0, 1]
func (r *HoltWinters) constrain(x *mutable.Float64Array) {
	// alpha
	if x.Value(0) > 1 {
		x.Set(0, 1)
	}
	if x.Value(0) < 0 {
		x.Set(0, 0)
	}
	// beta
	if x.Value(1) > 1 {
		x.Set(1, 1)
	}
	if x.Value(1) < 0 {
		x.Set(1, 0)
	}
	// gamma
	if x.Value(2) > 1 {
		x.Set(2, 1)
	}
	if x.Value(2) < 0 {
		x.Set(2, 0)
	}
	// phi
	if x.Value(3) > 1 {
		x.Set(3, 1)
	}
	if x.Value(3) < 0 {
		x.Set(3, 0)
	}
}
