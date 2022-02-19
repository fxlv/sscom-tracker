from rich.console import Console
from rich.table import Table

console = Console()


def print_results_to_console(results, category):
    # display results and send push notifications
    apartment_table = Table(title="Apartments")
    apartment_table.add_column("Apartment")
    apartment_table.add_column("Street")
    apartment_table.add_column("Rooms")
    apartment_table.add_column("Floor")
    apartment_table.add_column("Price")

    house_table = Table(title="Houses")
    house_table.add_column("House")
    house_table.add_column("Street")

    car_table = Table(title="Cars")
    car_table.add_column("Car")
    car_table.add_column("Model")
    car_table.add_column("Mileage")
    car_table.add_column("Year")
    car_table.add_column("Price")

    dog_table = Table(title="Dogs")
    dog_table.add_column("Dog")
    dog_table.add_column("Age")
    dog_table.add_column("Price")

    houses = []
    apartments = []
    dogs = []
    cars = []

    for classified in results:
        if classified.category == "house":
            houses.append(classified)
        if classified.category == "apartment":
            apartments.append(classified)
        if classified.category == "car":
            cars.append(classified)
        if classified.category == "dog":
            dogs.append(classified)

    # display the results
    for apartment in apartments:
        apartment_table.add_row(
            f"[bold red] {apartment.title}[/bold red]",
            apartment.street,
            str(apartment.rooms),
            str(apartment.floor),
            apartment.price,
        )

    for house in houses:
        house_table.add_row(f"[bold red] {house.title}[/bold red]", house.street)

    for car in cars:
        car_table.add_row(
            f"[bold red] {car.title}[/bold red]",
            car.model,
            car.mileage,
            str(car.year),
            str(car.price),
        )

    for dog in dogs:
        dog_table.add_row(f"[bold red] {dog.title}[/bold red]", dog.age, dog.price)

    if category == "house" or category == "*":
        console.print(house_table)
    if category == "car" or category == "*":
        console.print(car_table)
    if category == "dog" or category == "*":
        console.print(dog_table)
    if category == "apartment" or category == "*":
        console.print(apartment_table)
