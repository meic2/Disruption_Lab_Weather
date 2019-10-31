import cdsapi

c = cdsapi.Client()

c.retrieve(
    'projections-cmip5-monthly-single-levels',
    {
        'ensemble_member':'r1i1p1',
        'format':'zip',
        'variable':'mean_precipitation_flux',
        'experiment':'amip',
        'model':'giss_e2_r',
        'period':'195101-201012'
    },
    'mean_precipitation_flux.zip')