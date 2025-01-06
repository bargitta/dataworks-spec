package com.aliyun.dataworks.migrationx.domain.adf;

import com.aliyun.dataworks.migrationx.domain.dataworks.standard.service.AbstractPackageFileService;
import com.aliyun.migrationx.common.exception.BizException;
import com.aliyun.migrationx.common.exception.ErrorCode;
import com.aliyun.migrationx.common.utils.ZipUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.Optional;

public class AdfPackageFileService extends AbstractPackageFileService<AdfPackage> {
    protected AdfPackage adfPackage;
    File unzippedDir;


    @Override
    protected boolean isProjectRoot(File file) {
        return file.isFile() && (StringUtils.equals(file.getName(), "pipelines.json"));
    }

    public File getPath() {
        return unzippedDir;
    }

    @Override
    public void load(AdfPackage sourcePackage) throws Exception {
        File unzippedDir;
        if (sourcePackage.getPackageFile().isFile()) {
            unzippedDir = ZipUtils.decompress(sourcePackage.getPackageFile());
        } else {
            unzippedDir = sourcePackage.getPackageFile();
        }

        AdfPackageLoader loader = new AdfPackageLoader(unzippedDir);
        this.adfPackage = loader.loadPackage();
    }

    @Override
    public AdfPackage getPackage() throws Exception {
        return Optional.ofNullable(adfPackage)
                .orElseThrow(() -> new BizException(ErrorCode.PACKAGE_NOT_LOADED));

    }

    @Override
    public void write(AdfPackage adfPackage, File targetPackageFile) throws Exception {
        throw new UnsupportedOperationException("not implemented yet");
    }
}
