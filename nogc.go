package nogc

import (
	"fmt"
	"reflect"
	"runtime"
	"syscall"
	"unsafe"
)

type Error string

func (e Error) Error() string {
	return string(e)
}

func TypeValidate(i interface{}) error {
	v := reflect.ValueOf(i)
	if !v.IsValid() {
		return Error(fmt.Sprintf("invalid type: %#v", v))
	}
	return typeValidate(v.Type())
}

func typeValidate(t reflect.Type) error {
	switch k := t.Kind(); k {
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16,
		reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16,
		reflect.Uint32, reflect.Uint64, reflect.Uintptr, reflect.Float32,
		reflect.Float64, reflect.Complex64, reflect.Complex128, reflect.UnsafePointer:
		return nil
	case reflect.Array:
		return typeValidate(t.Elem())
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			if err := typeValidate(t.Field(i).Type); err != nil {
				return err
			}
		}
		return nil
	default:
		return Error(fmt.Sprintf("invalid type: %#v", k.String()))
	}
}

type Datum struct {
	chunkSize uintptr
	objSize   uintptr

	slice reflect.Value
	bytes []byte
}

func (dt Datum) NbObjects() uint {
	return uint(dt.chunkSize / dt.objSize)
}

func (dt *Datum) Read(i int) interface{} {
	return dt.slice.Index(i).Interface()
}

func (dt *Datum) Write(i int, v interface{}) interface{} {
	val := reflect.ValueOf(v)
	if val.Type() != dt.slice.Type().Elem() {
		panic("illegal value")
	}
	dt.slice.Index(i).Set(val)
	return v
}

func (dt Datum) Pointer(i int) uintptr {
	return uintptr(unsafe.Pointer(&(dt.bytes[uintptr(i)*dt.objSize])))
}

func NewDatum(v interface{}, n uint) (Datum, error) {
	if n == 0 {
		return Datum{}, Error("`n` must be > 0")
	}
	if err := TypeValidate(v); err != nil {
		return Datum{}, err
	}

	t := reflect.TypeOf(v)
	size := t.Size()
	bytes, err := syscall.Mmap(
		0, 0, int(size*uintptr(n)),
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_PRIVATE,
	)
	if err != nil {
		return Datum{}, err
	}

	itf := reflect.MakeSlice(reflect.SliceOf(t), int(n), int(n)).Interface()
	si := (*reflect.SliceHeader)((*[2]unsafe.Pointer)(unsafe.Pointer(&itf))[1])
	si.Data = uintptr(unsafe.Pointer(&bytes[0]))

	slice := reflect.ValueOf(itf)
	for i := 0; i < slice.Len(); i++ {
		slice.Index(i).Set(reflect.ValueOf(v))
	}

	ret := Datum{
		chunkSize: size * uintptr(n),
		objSize:   size,

		slice: slice,
		bytes: bytes,
	}

	runtime.SetFinalizer(&ret, func(chunk *Datum) {
		if chunk.bytes != nil {
			chunk.Delete()
		}
	})

	return ret, nil
}

func (dt *Datum) Delete() error {
	err := syscall.Munmap(dt.bytes)
	if err != nil {
		return err
	}

	dt.chunkSize = 0
	dt.objSize = 1
	dt.bytes = nil

	return nil
}
