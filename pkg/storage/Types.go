package storage

type IHashable interface {
	Hash() (key string, searchKeys []string)
}
