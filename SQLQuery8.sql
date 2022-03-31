-- Select data that we are going to be using

SELECT location,date,total_cases,new_cases,total_deaths,population
FROM CovidAnalysis..CovidDeaths
WHERE continent is not null
ORDER BY 1,2

-- Total Cases vs Total Deaths
-- Likelihood of dying if infected over the period

SELECT location,date,total_cases,new_cases,total_deaths,
		(total_deaths/total_cases)*100 AS DeathPercentage
FROM CovidAnalysis..CovidDeaths
ORDER BY 1,2 

-- Total Cases vs Population
-- Shows the population infected over the period

SELECT location,date,total_cases,new_cases,population,
		(total_cases/population)*100 AS CasePercentage
FROM CovidAnalysis..CovidDeaths
WHERE continent is not null
ORDER BY 1,2

-- Countries with highest infection rate compared to population

SELECT location,population,MAX(total_cases) AS HighestInfectionCount,
		MAX((total_cases/population))*100 AS InfectionPercentage
FROM CovidAnalysis..CovidDeaths
WHERE continent is not null
GROUP BY location,population
ORDER BY 4 DESC

-- Countries with highest death rate compared to population

SELECT location,population,MAX(CAST(total_deaths AS int)) AS HighestDeathCount,
		MAX((total_deaths/population))*100 AS DeathPercentage
FROM CovidAnalysis..CovidDeaths
WHERE continent is not null
GROUP BY location,population
ORDER BY 3 DESC

-- Continents with highest death rate compared to population

SELECT continent,MAX(CAST(total_deaths AS int)) AS HighestDeathCount
FROM CovidAnalysis..CovidDeaths
WHERE continent is not null
GROUP BY continent
ORDER BY 2 DESC

--Global Numbers

SELECT date,
SUM(new_cases) AS DailyNewCases,
SUM(CAST(new_deaths AS int)) AS DailyNewDeaths,
(SUM(CAST(new_deaths AS int))/SUM(new_cases))*100 AS DailyDeathPercentage
FROM CovidAnalysis..CovidDeaths
WHERE continent is not null
GROUP BY date
ORDER BY 2

-- Total Covid cases, deaths and death percentage of all time

SELECT 
SUM(new_cases) AS TotalCasesWorldwide,
SUM(CAST(new_deaths AS int)) AS TotalDeathsWorldwide,
(SUM(CAST(new_deaths AS int))/SUM(new_cases))*100 AS DailyDeathPercentage
FROM CovidAnalysis..CovidDeaths
WHERE continent is not null

-- Total number of people vaccinated per day

SELECT death.date, SUM(CAST(vaccine.new_vaccinations AS int))
FROM CovidAnalysis..CovidDeaths death
INNER JOIN CovidVaccinations vaccine
ON death.location = vaccine.location
and death.date = vaccine.date
WHERE death.continent is not null and vaccine.new_vaccinations > 0
GROUP BY death.date
ORDER BY 2 DESC

-- Total number of people vaccinated location wise

SELECT death.continent, death.location, death.date, death.population, CAST(vaccine.new_vaccinations AS int) AS NewVaccinations, 
	SUM(CAST(new_vaccinations AS bigint)) OVER (PARTITION BY death.location ORDER BY death.location, death.date) AS RollingPeopleVaccinated
FROM CovidAnalysis..CovidDeaths death
INNER JOIN CovidVaccinations vaccine
ON death.location = vaccine.location
and death.date = vaccine.date
WHERE death.continent is not null and vaccine.new_vaccinations is not null
ORDER BY death.location

-- CTE

WITH PeopleVaccinated (continent, location, date, population, new_vaccinations, RollingPeopleVaccinated)
AS
(
SELECT death.continent, death.location, death.date, death.population, CAST(vaccine.new_vaccinations AS int) AS NewVaccinations, 
	SUM(CAST(new_vaccinations AS bigint)) OVER (PARTITION BY death.location ORDER BY death.location, death.date) AS RollingPeopleVaccinated
FROM CovidAnalysis..CovidDeaths death
INNER JOIN CovidVaccinations vaccine
ON death.location = vaccine.location
and death.date = vaccine.date
WHERE death.continent is not null and vaccine.new_vaccinations is not null
)
SELECT *, (RollingPeopleVaccinated/Population)*100 AS VaccinationRate
FROM PeopleVaccinated
ORDER BY 1,2

-- Temp Table

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
SELECT death.continent, death.location, death.date, death.population, CAST(vaccine.new_vaccinations AS int) AS NewVaccinations, 
	SUM(CAST(new_vaccinations AS bigint)) OVER (PARTITION BY death.location ORDER BY death.location, death.date) AS RollingPeopleVaccinated
FROM CovidAnalysis..CovidDeaths death
INNER JOIN CovidVaccinations vaccine
ON death.location = vaccine.location
and death.date = vaccine.date
WHERE death.continent is not null and vaccine.new_vaccinations is not null

SELECT *, (RollingPeopleVaccinated/population)*100 AS VaccinationRate
FROM #PeopleVaccinated

-- Creating view to store data for later visualizations

CREATE VIEW PeopleVaccinated AS 
SELECT death.continent, death.location, death.date, death.population, CAST(vaccine.new_vaccinations AS int) AS NewVaccinations, 
	SUM(CAST(new_vaccinations AS bigint)) OVER (PARTITION BY death.location ORDER BY death.location, death.date) AS RollingPeopleVaccinated
FROM CovidAnalysis..CovidDeaths death
INNER JOIN CovidVaccinations vaccine
ON death.location = vaccine.location
and death.date = vaccine.date
WHERE death.continent is not null and vaccine.new_vaccinations is not null