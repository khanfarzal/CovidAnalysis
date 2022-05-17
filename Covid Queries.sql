-- Discrete Queries 
-- Country-wise information daily
-- Country-wise Total Deaths vs Total Cases daily

SELECT continent, location, date, 
	total_cases AS TotalCases, -- Gives the total number of cases
	CAST(total_deaths AS bigint) AS TotalDeaths, --Gives the total number of deaths
	CAST(total_deaths AS float)/total_cases*100 AS PercentInfectionDeath -- Calculates the percentage of infected people who died
FROM CovidAnalysis..CovidDeaths
WHERE continent is not null
ORDER BY 1,2,3

-- Country-wise Total Cases vs Population daily

SELECT continent, location, date, population, 
	total_cases AS TotalCases, -- Gives the total number of cases
	CAST(total_cases AS float)/population*100 AS PercentPopulationInfected -- Calculates the percentage of population infected
FROM CovidAnalysis..CovidDeaths
WHERE continent is not null
ORDER BY 1,2,3

-- Country-wise Total Deaths vs Population daily

SELECT continent, location, date, population, 
	CAST(total_deaths AS bigint) AS TotalDeaths, -- Gives the total number of deaths
	CAST(total_deaths AS float)/population*100 AS PercentPopulationDeath -- Calculates the percentage of population died
FROM CovidAnalysis..CovidDeaths
WHERE continent is not null
ORDER BY 1,2,3

-- Country-wise Total Vaccinations daily

SELECT d.continent, d.location, d.date, d.population, 
	CAST(v.new_vaccinations AS int) AS NewVaccinations, -- Gives the total number of vaccinations
	SUM(CAST(v.new_vaccinations AS float)) OVER (PARTITION BY d.location ORDER BY d.location, d.date) AS RollingPeopleVaccinated -- Calculates rolling sum of the total number of vaccinations
FROM CovidAnalysis..CovidDeaths AS d
INNER JOIN CovidAnalysis..CovidVaccinations AS v
ON d.location = v.location
and d.date = v.date
WHERE d.continent is not null AND v.new_vaccinations is not null
ORDER BY 1,2,3

-- Daily Vaccination Rate of countries using CTE

WITH PeopleVaccinated (continent, location, date, population, new_vaccinations, RollingPeopleVaccinated)
AS
(
SELECT death.continent, death.location, death.date, death.population, CAST(vaccine.new_vaccinations AS bigint) AS NewVaccinations, 
	SUM(CAST(new_vaccinations AS float)) OVER (PARTITION BY death.location ORDER BY death.location, death.date) AS RollingPeopleVaccinated
FROM CovidAnalysis..CovidDeaths death
INNER JOIN CovidAnalysis..CovidVaccinations vaccine
ON death.location = vaccine.location
and death.date = vaccine.date
WHERE death.continent is not null
)
SELECT *, (RollingPeopleVaccinated/Population)*100 AS VaccinationRate -- Calculates the percentage of population vaccinated
FROM PeopleVaccinated
ORDER BY 1,2

-- Daily Vaccination Rate of countries using Temp Table

DROP TABLE if exists #PeopleVaccinated
CREATE TABLE #PeopleVaccinated
(
continent nvarchar(255),
location nvarchar(255),
date datetime,
population numeric,
new_vaccinations numeric,
RollingPeopleVaccinated numeric
)

INSERT INTO #PeopleVaccinated
SELECT death.continent, death.location, death.date, death.population, CAST(vaccine.new_vaccinations AS bigint) AS NewVaccinations, 
	SUM(CAST(new_vaccinations AS float)) OVER (PARTITION BY death.location ORDER BY death.location, death.date) AS RollingPeopleVaccinated
FROM CovidAnalysis..CovidDeaths death
INNER JOIN CovidAnalysis..CovidVaccinations vaccine
ON death.location = vaccine.location
and death.date = vaccine.date
WHERE death.continent is not null

SELECT *, (RollingPeopleVaccinated/population)*100 AS VaccinationRate
FROM #PeopleVaccinated

-- Time series of stringency index and tets per case

SELECT continent, location, date, 
	stringency_index, -- Gives the percentage of non-essential services banned
	tests_per_case -- Gives the number of deaths per case
FROM CovidAnalysis..CovidVaccinations
WHERE continent is not null
ORDER BY 1,2,3

-- Country-wise information over the period
-- Country-wise Total Deaths vs Total Cases over the period

SELECT continent, location, 
	MAX(total_cases) AS TotalCases, -- Gives the total number of cases
	MAX(CAST(total_deaths AS bigint)) AS TotalDeaths, -- Gives the total number of deaths
	MAX(CAST(total_deaths AS float))/MAX(total_cases)*100 AS PercentInfectionDeath -- Gives the percentage of infected people who died
FROM CovidAnalysis..CovidDeaths
WHERE continent is not null
GROUP BY continent, location
ORDER BY 1,2

-- Country-wise Total Cases vs Population over the period

SELECT continent, location, population, 
	MAX(total_cases) AS TotalCases, -- Gives the total number of cases
	MAX(total_cases/population)*100 AS PercentPopulationInfected -- Gives the percentage of population infected
FROM CovidAnalysis..CovidDeaths
WHERE continent is not null
GROUP BY continent, location, population
ORDER BY 1,2

-- Country-wise Total Deaths vs Population over the period

SELECT continent, location, population, 
	MAX(CAST(total_deaths AS bigint)) AS TotalDeaths, -- Gives the total number of deaths
	MAX(CAST(total_deaths AS float)/population)*100 AS PercentPopulationDeath -- Gives the percentage of population died
FROM CovidAnalysis..CovidDeaths
WHERE continent is not null
GROUP BY continent, location, population
ORDER BY 1,2

-- Country-wise Total Vaccinations vs Population over the period

SELECT d.continent, d.location, d.population, 
	MAX(CAST(v.total_vaccinations AS bigint)) AS TotalVaccinations, -- Gives the total number of vaccinations
	MAX(CAST(v.total_vaccinations AS float)/d.population)*100 AS PercentPopulationVaccinated -- Calculates the percentage of population vaccinated
FROM CovidAnalysis..CovidDeaths AS d
INNER JOIN CovidAnalysis..CovidVaccinations AS v
ON d.location = v.location
WHERE d.continent is not null
GROUP BY d.continent, d.location, d.population
ORDER BY 1,2

-- Country-wise Partial Vaccinations vs Population over the period using CTE

WITH PartialVaccinations (continent, location, population, PartiallyVaccinated)
AS
(
	SELECT d.continent, d.location, d.population, 
		MAX(CAST(v.people_vaccinated AS float))-MAX(CAST(v.people_fully_vaccinated AS float)) AS PartiallyVaccinated -- Gives the total number of people partially vaccinated
	FROM CovidAnalysis..CovidDeaths AS d
	INNER JOIN CovidAnalysis..CovidVaccinations AS v
	ON d.location = v.location
	WHERE d.continent is not null
	GROUP BY d.continent, d.location, d.population
)
SELECT *, PartiallyVaccinated/population*100 AS PercentPartiallyVaccinated -- Calculates the percentage of population partially vaccinated
FROM PartialVaccinations
ORDER BY 1,2

-- Country-wise Full Vaccinations vs Population over the period

SELECT d.continent, d.location, d.population, 
	MAX(CAST(v.people_fully_vaccinated AS bigint)) AS FullyVaccinated, -- Gives the total number of people fully vaccinated
	MAX(CAST(v.people_fully_vaccinated AS float))/d.population*100 AS PercentFullyVaccinated -- Calculates the percentage of population fully vaccinated
FROM CovidAnalysis..CovidDeaths AS d
INNER JOIN CovidAnalysis..CovidVaccinations AS v
ON d.location = v.location
WHERE d.continent is not null
GROUP BY d.continent, d.location, d.population
ORDER BY 1,2

-- Country-wise Total Boosters vs Population over the period

SELECT d.continent, d.location, d.population, 
	MAX(CAST(v.total_boosters AS bigint)) AS TotalBoosters, -- Gives the total number of people who took booster shots
	MAX(CAST(v.total_boosters AS float))/d.population*100 AS PercentTotalBoosters -- Calculates the percentage of population who took booster shots
FROM CovidAnalysis..CovidDeaths AS d
INNER JOIN CovidAnalysis..CovidVaccinations AS v
ON d.location = v.location
WHERE d.continent is not null
GROUP BY d.continent, d.location, d.population
ORDER BY 1,2

-- Global Numbers
-- Continent-wise Total Deaths over the period

SELECT location, MAX(CAST(total_deaths AS bigint)) AS TotalDeaths
FROM CovidAnalysis..CovidDeaths
WHERE location in ('Asia', 'North America', 'South America', 'Europe', 'Africa', 'Oceania')
GROUP BY location
ORDER BY 2 DESC

-- Total Covid Cases, Deaths and Death Percentage worldwide

SELECT 
SUM(new_cases) AS TotalCasesWorldwide,
SUM(CAST(new_deaths AS int)) AS TotalDeathsWorldwide,
SUM(CAST(new_deaths AS int))/SUM(new_cases)*100 AS PercentInfectionDeath
FROM CovidAnalysis..CovidDeaths
WHERE continent is not null

-- Problems in dataset
-- total_deaths column adds numbers for the corresponding null values in the new_deaths column
-- Rolling sum of new deaths daily and the total deaths do not match

SELECT location, date, 
	new_deaths AS NewDeaths, 
	SUM(CAST(new_deaths AS bigint)) OVER (PARTITION BY location ORDER BY location, date) RollingDeathSum, -- Gives rolling sum of new deaths
	total_deaths AS TotalDeaths -- Gives the total number of deaths
FROM CovidAnalysis..CovidDeaths
WHERE continent is not null
GROUP BY location, date, new_deaths, total_deaths

------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Merged Queries
-- Organizing the data and feeding my OCD until it feels 'Just Right'
-- Better used for visualization

-- Country Statistics Daily

SELECT  d.continent AS Continent,
	d.location AS Country, 
	d.date AS Date,
	d.population AS Population,
	d.new_cases AS DailyCases, -- Gives the number of new cases
	d.total_cases AS TotalCases, -- Gives the total number of cases
	CAST(d.total_cases AS float)/d.population*100 AS PercentPopulationInfected, -- Calculates the percentage of population infected
	CAST(d.new_deaths AS bigint) AS NewDeaths, -- Gives the number of new deaths
	CAST(d.total_deaths AS bigint) AS TotalDeaths, -- Gives the total number of deaths
	CAST(d.total_deaths AS float)/d.population*100 AS PercentPopulationDeath, -- Calculates the percentage of population dead
	CAST(d.total_deaths AS float)/d.total_cases*100 AS PercentInfectionDeath, -- Calculates the percentage of infected people who died
	CAST(d.reproduction_rate AS float) AS ReproductionRate, 
	CAST(d.icu_patients AS int) AS ICUPatients, -- Gives the number of ICU admissions
	CAST(d.hosp_patients AS int) AS HospitalPatients, -- Gives the number of hospitalized patients
	CAST(v.new_tests_smoothed AS bigint) AS NewTests, -- Gives the number of new tests
	CAST(v.total_tests AS bigint) AS TotalTests, -- Gives the number of total tests
	CAST(v.positive_rate AS float) AS PositiveRate, -- Number of tests done to find one covid positive case -- Calculated as inverse of testspercase (1/testspercase)
	CAST(v.tests_per_case AS float) AS TestsPerCase, -- Gives the number of tests per case -- Calculated as new_tests_smoothed/new_cases_smoothed
	CAST(v.new_vaccinations_smoothed AS bigint) AS NewVaccinations, -- Gives the total number of vaccinations
	SUM(CAST(v.new_vaccinations AS bigint)) OVER (PARTITION BY d.location ORDER BY d.location, d.date) AS RollingPeopleVaccinated, -- Calculates rolling sum of the total number of vaccinations -- Not reliable, use TotalVaccinations instead
	CAST(v.total_vaccinations AS bigint) AS TotalVaccinations, -- Gives the total number of vaccinations
	CAST(v.total_vaccinations AS float)/d.Population*100 AS PercentPopulationVaccinated, -- Calculates the percentage of population vaccinated
	CAST(v.people_vaccinated AS bigint) AS AtleastFirstDose, -- Gives the total number of people with atleast first dose
	CAST(v.people_vaccinated AS float)/d.population*100 AS PercentAtleastFirstDose, -- Calculates the percentage of population with atleast first dose
	CAST(v.people_vaccinated AS bigint)-CAST(v.people_fully_vaccinated AS bigint) AS PartiallyVaccinated, -- Gives the total number of people partially vaccinated
        (CAST(v.people_vaccinated AS float)-CAST(v.people_fully_vaccinated AS float))/d.population*100 AS PercentPartiallyVaccinated, -- Calculates the percentage of population partially vaccinated
	CAST(v.people_fully_vaccinated AS bigint) AS FullyVaccinated, -- Gives the total number of people fully vaccinated
	CAST(v.people_fully_vaccinated AS float)/d.population*100 AS PercentFullyVaccinated, -- Calculates the percentage of population fully vaccinated
	CAST(v.total_boosters AS bigint) AS TotalBoosters, -- Gives the total number of people who took booster shots
	CAST(v.total_boosters AS float)/d.population*100 AS PercentTotalBoosters, -- Calculates the percentage of population who took booster shots
	v.stringency_index AS StringencyIndex-- Gives the percentage of non-essential services banned
FROM CovidAnalysis..CovidDeaths AS d
INNER JOIN CovidAnalysis..CovidVaccinations AS v
ON d.date = v.date AND d.location = v.location
WHERE d.location not in ('Lower Middle Income', 'World', 'Low Income', 'European Union', 'International', 'Upper Middle Income', 'High Income') 
ORDER BY 1,2,3

-- Country Statistics Over The Period using CTE
-- Did not comment each line because they all work similar as above

WITH CountryTotal (Continent, Country, Population, TotalCases, TotalDeaths, AverageReproductionRate, AverageICUPatients, 
	AverageHospitalPatients, AveragePositiveRate, AverageTestsPerCase, TotalTests, TotalVaccinations, AtleastFirstDose, 
	PartiallyVaccinated, FullyVaccinated, TotalBoosters, MedianAge, AgeAbove65, AgeAbove70, GDPPerCapita, ExtremePoverty, 
	CardiovascDeathRate, DiabetesPrevalence, FemaleSmokers, MaleSmokers, HandwashingFacilities, 
	HospitalBedsPerthousand, LifeExpectancy, HumanDevelopmentIndex)
AS
(
	SELECT  d.continent AS Continent,
		d.location AS Country, 
		MAX(d.population) AS Population,
		MAX(d.total_cases) AS TotalCases, 
		MAX(CAST(d.total_deaths AS bigint)) AS TotalDeaths,
		AVG(CAST(d.reproduction_rate AS float)) AS AverageReproductionRate,
		AVG(CAST(d.icu_patients AS bigint)) AS AverageICUPatients,
		AVG(CAST(d.hosp_patients AS bigint)) AS AverageHospitalPatients,
		MAX(CAST(v.total_tests AS bigint)) AS TotalTests,
		MAX(CAST(v.total_vaccinations AS bigint)) AS TotalVaccinations,
		MAX(CAST(v.people_vaccinated AS bigint)) AS AtleastFirstDose,
		MAX(CAST(v.people_vaccinated AS bigint))-MAX(CAST(v.people_fully_vaccinated AS bigint)) AS PartiallyVaccinated,
		MAX(CAST(v.people_fully_vaccinated AS bigint)) AS FullyVaccinated,
		MAX(CAST(v.total_boosters AS bigint)) AS TotalBoosters,
		MAX(v.median_age) AS MedianAge, 
		MAX(v.aged_65_older) AS AgeAbove65,
		MAX(v.aged_70_older) AS AgeAbove70,
		MAX(v.gdp_per_capita) AS GDPPerCapita,
		MAX(CAST(v.extreme_poverty AS float)) AS ExtremePoverty,
		MAX(v.cardiovasc_death_rate) AS CardiovascDeathRate,
		MAX(v.diabetes_prevalence) AS DiabetesPrevalence,
		MAX(CAST(v.female_smokers AS float)) AS FemaleSmokers,
		MAX(CAST(v.male_smokers AS float)) AS MaleSmokers,
		MAX(v.handwashing_facilities) AS HandwashingFacilities,
		MAX(v.hospital_beds_per_thousand) AS HospitalBedsPerthousand,
		MAX(v.life_expectancy) AS LifeExpectancy,
		MAX(v.human_development_index) AS HumanDevelopmentIndex
	FROM CovidAnalysis..CovidDeaths AS d
	INNER JOIN CovidAnalysis..CovidVaccinations AS v
	ON d.location = v.location
	WHERE d.continent is not null
	GROUP BY d.continent, d.location, d.population
)
SELECT  Continent, Country, Population, 
	TotalCases, 
	TotalCases/Population*100 AS PercentPopulationInfected,
	TotalDeaths,
	TotalDeaths/Population*100 AS PercentPopulationDeath,
	TotalDeaths/TotalCases*100 AS PercentInfectionDeath,
	AverageReproductionRate,
	AverageICUPatients,
	AverageHospitalPatients
	TotalTests,
	AveragePositiveRate,
	AverageTestsPerCase,
	TotalVaccinations,
	TotalVaccinations/Population*100 AS PercentPopulationVaccinated,
	AtleastFirstDose,
	AtleastFirstDose/Population*100 AS PercentAtleastFirstDose,
	PartiallyVaccinated,
	PartiallyVaccinated/Population*100 AS PercentPartiallyVaccinated,
	FullyVaccinated,
	FullyVaccinated/Population*100 AS PercentFullyVaccinated,
	TotalBoosters,
	TotalBoosters/Population*100 AS PercentTotalBoosters,
	MedianAge, 
	AgeAbove65, 
	AgeAbove70, 
	GDPPerCapita, 
	ExtremePoverty, 
	CardiovascDeathRate, 
	DiabetesPrevalence, 
	FemaleSmokers, 
	MaleSmokers, 
	HandwashingFacilities, 
	HospitalBedsPerthousand, 
	LifeExpectancy, 
	HumanDevelopmentIndex

FROM CountryTotal
ORDER BY 1,2

-- Continent Statistics Over The Period using CTE

WITH ContinentTotal(Country, Date, Population, TotalCases, TotalDeaths, TotalTests, PositiveRate, TestsPerCase, TotalVaccinations, AtleastFirstDose, PartiallyVaccinated, FullyVaccinated, TotalBoosters)
AS
(
	SELECT  d.location AS Country, d.date AS Date, 
		d.population AS Population,
		d.total_cases AS TotalCases, 
		CAST(d.total_deaths AS bigint) AS TotalDeaths, 
		CAST(v.total_tests AS bigint) AS TotalTests,
		CAST(v.positive_rate AS float) AS PositiveRate,
		CAST(v.tests_per_case AS float) AS TestsPerCase,
		CAST(v.total_vaccinations AS bigint) AS TotalVaccinations,
		CAST(v.people_vaccinated AS bigint) AS AtleastFirstDose,
		CAST(v.people_vaccinated AS bigint)-CAST(v.people_fully_vaccinated AS bigint) AS PartiallyVaccinated,
		CAST(v.people_fully_vaccinated AS bigint) AS FullyVaccinated,
		CAST(v.total_boosters AS bigint) AS TotalBoosters
	FROM CovidAnalysis..CovidDeaths AS d
	INNER JOIN CovidAnalysis..CovidVaccinations AS v
	ON d.date = v.date AND d.location = v.location
	WHERE d.location in ('Asia', 'North America', 'South America', 'Europe', 'Africa', 'Oceania')
)
SELECT  Country, Date, Population, 
	TotalCases, 
	TotalCases/Population*100 AS PercentPopulationInfected,
	TotalDeaths,
	TotalDeaths/Population*100 AS PercentPopulationDeath,
	TotalDeaths/TotalCases*100 AS PercentInfectionDeath,
	TotalVaccinations,
	TotalVaccinations/Population*100 AS PercentPopulationVaccinated,
	AtleastFirstDose,
	AtleastFirstDose/Population*100 AS PercentAtleastFirstDose,
	PartiallyVaccinated,
	PartiallyVaccinated/Population*100 AS PercentPartiallyVaccinated,
	FullyVaccinated,
	FullyVaccinated/Population*100 AS PercentFullyVaccinated,
	TotalBoosters,
	TotalBoosters/Population*100 AS PercentTotalBoosters
FROM ContinentTotal
ORDER BY 1,2

-- Global Numbers Over The Period

SELECT  d.location, d.date, d.population AS Population,
	CAST(d.total_cases AS bigint) AS TotalCases,
	CAST(d.total_cases AS bigint)/d.population*100 AS PercentPopulationInfected,
	CAST(d.total_deaths AS bigint) AS TotalDeaths,
	CAST(d.total_deaths AS bigint)/d.population*100 AS PercentPopulationDead,
	CAST(d.total_deaths AS float)/CAST(d.total_cases AS float)*100 AS PercentInfectionDeath,
	CAST(d.reproduction_rate AS float) AS ReproductionRate,
	CAST(v.total_vaccinations AS bigint) AS TotalVaccinations,
	CAST(v.total_vaccinations AS bigint)/d.population AS PercentPopulationVaccinated,
	CAST(v.people_vaccinated AS bigint) AS AtleastFirstDose,
	CAST(v.people_vaccinated AS bigint)/d.population AS PercentAtleastFirstDose,
	CAST(v.people_vaccinated AS bigint)-CAST(v.people_fully_vaccinated AS bigint) AS PartiallyVaccinated,
   	(CAST(v.people_vaccinated AS bigint)-CAST(v.people_fully_vaccinated AS bigint))/d.population*100 AS PercentPartiallyVaccinated,
	CAST(v.people_fully_vaccinated AS bigint) AS FullyVaccinated,
	CAST(v.people_fully_vaccinated AS bigint)/d.population AS PercentFullyVaccinated,
	CAST(v.total_boosters AS bigint) AS TotalBoosters,
	CAST(v.total_boosters AS float)/d.population*100 AS PercentTotalBoosters
FROM CovidAnalysis..CovidDeaths AS d
INNER JOIN CovidAnalysis..CovidVaccinations AS v
ON d.date = v.date AND d.location = v.location
WHERE d.location like '%World%'
