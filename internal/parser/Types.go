package parser

type IParseAble[T any] interface {
	Parse(record map[string]string) T
}
