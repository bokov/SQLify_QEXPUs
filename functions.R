unziprecursive <- function(file, skip=c('xlsx','docx'),outdir=tempdir()
                           ,cleanup=c('excepttop','yes','no')
                           ,printfiles=T
                           ,.recursion=0
){
  # capture function for recursive calling
  .thisfun <- sys.function(); 
  cleanup <- match.arg(cleanup);
  if(!.recursion && 
     file.exists(outdir) &&
     tools::file_path_as_absolute(dirname(file)) == tools::file_path_as_absolute(outdir)){
    stop('For the safety of your files, you cannot have the original file be in the same location as the output files specified by the "outdir" argument. Aborting');
  } 
  # if the file is unzippable, derive the name of the output subdirectory
  newsubdir <- gsub("([^.]+)\\.[a-zA-Z0-9]{,5}$",'\\1',basename(file));
  # if that name collides with the file itself, append a suffix
  if(newsubdir==basename(file)) newsubdir <- paste0(newsubdir,'_x');
  newoutdir <- file.path(outdir,newsubdir);
  # if the file matches any of the skip suffixes, do nothing 
  if(tools::file_ext(file) %in% skip) return();
  # otherwise...
  # attempt to unzip to newoutdir and retain a list of contained files
  suppressWarnings(containedfiles <- try(unzip(file,exdir = newoutdir),silent=T));
  # if unzip fails, exit
  if(is(containedfiles,'try-error')||is.null(containedfiles)) return();
  # otherwise, run .thisfun on each of the contained but set the outdir to 
  # newoutdir; if cleanup was excepttop, change it to yes; increment .recursion
  for(ii in containedfiles[!tools::file_ext(containedfiles) %in% skip]){
    .thisfun(ii,skip=skip,outdir=newoutdir,printfiles=printfiles
             ,cleanup=if(cleanup=='excepttop') 'yes' else cleanup
             ,.recursion=.recursion+1)};
  # remove zip files if requested but only if they are somewhere in the outdir,
  # NOT any original input files
  if(cleanup=='yes' && file %in% list.files(outdir,full.names=T,recursive = T)){
    unlink(file)};
  # if recursion = 0 list.files on newoutdir
  if(!.recursion && printfiles) list.files(newoutdir,full.names = T,recursive = T);
}

sqlify2 <- function(dat,colformat='[{`xx`}]',suffixfirst=' INTO ##sqlifyoutput ',prefixfirst=''){
  dat <- mutate(dat,across(c(where(is.character),where(is.factor)),~gsub("'","''",.x)));
  out <- sapply(names(dat),function(xx) 
    case_when(is(dat[[xx]],'character')|is(dat[[xx]],'factor')~str_glue(paste0(" '{{`{`xx`}`}}' ",colformat))
              ,is(dat[[xx]],'numeric') ~ str_glue(paste0(" {{`{`xx`}`}} ",colformat))
              ,is(dat[[xx]],'POSIXt') ~ str_glue(" CONVERT(DATETIME,'{{`{`xx`}`}}',20) ",colformat)
              ,is(dat[[xx]],'logical') ~ str_glue(" {{`{`xx`}`+0}} ",colformat)
              ,TRUE~str_glue(paste0(" '{{`{`xx`}`}}' ",colformat))
    )) %>% sapply(function(xx) with(dat,str_glue(xx))); #str_glue(.envir=dat)) %>% 
  out <- apply(out,1,paste,collapse=',') %>% paste('SELECT ',.);
  # sapply(paste,collapse=',') %>% paste('SELECT ',.)
  out[1] <- paste0(prefixfirst,out[1],suffixfirst);
  paste0(out,collapse=' \nUNION ALL ') %>% gsub(" NA | 'NA' ",' NULL ',.) 
}


