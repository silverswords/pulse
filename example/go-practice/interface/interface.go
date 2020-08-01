package _interface

type People struct {
	age int
}

func (p *People) Age() int {
	return p.age
}

type Dog struct {
	age int
}

func (p *Dog) Age() int {
	return p.age
}

type Ager interface {
	Age() int
}

func Age(a Ager) int {
	return a.Age()
}
func DogAge(d Dog) int {
	return d.Age()
}
func PeopleAge(p People) int {
	return p.Age()
}
