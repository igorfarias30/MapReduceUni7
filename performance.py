
import numpy
import perfplot

perfplot.show(
    setup = lambda n: numpy.random.rand(n),
    kernels=[
        max,
        numpy.max,
        lambda x: x.max()
        ],
    labels=['max(x)', 'numpy.max(x)', 'x.max()'],
    n_range=[2**k for k in range(15)],
    logx=True,
    logy=True,
    xlabel='len(x)'
)
