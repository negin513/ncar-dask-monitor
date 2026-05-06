SHELL := /bin/bash

config_env := module load conda >/dev/null 2>&1 || true


conda: conda-env.yml
	rm -rf ./conda-env
	$(config_env) && conda env create --file $< --prefix ./conda-env -y
	@echo "Activate with: conda activate ./conda-env"

clean:
	rm *~

clobber:
	git clean -xdf --exclude ".env" --exclude "conda-env/"

distclean:
	$(MAKE) clobber
	rm -rf conda-env/


%.py : %.ipynb Makefile
	jupyter nbconvert --clear-output $<
	jupyter nbconvert --to=python $< >/dev/null
	chmod +x $@
	git add $@ $<

%: %.yml
	[ -d $@ ] && mv $@ $@.old && rm -rf $@.old &
	$(MAKE) solve-$*
	$(config_env) && conda env create --file $< --prefix ./$@
	$(config_env) && conda activate ./$@ && conda-tree deptree --small 2>/dev/null || /bin/true
	$(config_env) && conda activate ./$@ && pipdeptree --all 2>/dev/null || /bin/true
	$(config_env) && conda activate ./$@ && conda list

solve-%: %.yml
	$(config_env) && conda env create --file $< --prefix ./$@ --dry-run


